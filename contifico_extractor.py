from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Any, Iterable

import requests

try:
    from openpyxl import Workbook
except Exception:  # pragma: no cover - optional dependency fallback
    Workbook = None


ISO_DATE = "%Y-%m-%d"
EU_DATE = "%d/%m/%Y"
DEFAULT_BASE_URL = "https://api.contifico.com"
DEFAULT_TIMEOUT = 60
DEFAULT_RETRIES = 3
DEFAULT_OVERLAP_DAYS = 2
DEFAULT_MAX_WORKERS = 4


PRIMARY_KEYS: dict[str, list[str]] = {
    "personas": ["id"],
    "productos": ["id"],
    "movimientos": ["id"],
    "movimiento_detalles": ["movimiento_id", "detalle_index"],
    "documentos": ["id"],
    "documento_detalles": ["documento_id", "detalle_index"],
    "documento_cobros": ["documento_id", "cobro_index"],
    "tickets_documentos": ["id"],
    "tickets_detalles": ["documento_id", "detalle_index"],
    "tickets_items": ["documento_id", "detalle_index", "ticket_index"],
    "asientos": ["id"],
    "asiento_detalles": ["asiento_id", "detalle_index"],
    "periodos": ["id"],
    "categorias": ["id"],
    "bodegas": ["id"],
    "unidades": ["id"],
    "marcas": ["id"],
    "cuentas_contables": ["id"],
    "centros_costo": ["id"],
    "extract_runs": ["run_id", "resource"],
    "watermarks": ["resource"],
}


INCREMENTAL_RESOURCES = {
    "persona",
    "producto",
    "movimiento-inventario",
    "documento",
    "contabilidad/asiento",
}


SNAPSHOT_RESOURCES = {
    "unidad",
    "contabilidad/periodo",
    "categoria",
    "bodega",
    "marca",
    "cuenta-contable",
    "centro-costo",
}


@dataclass(frozen=True)
class ResourceSpec:
    key: str
    display_name: str
    path: str
    response_kind: str
    mode: str
    table_names: tuple[str, ...]
    date_format: str | None = None
    date_start_param: str = "fecha_inicio"
    date_end_param: str = "fecha_fin"
    record_date_field: str | None = None
    record_date_format: str | None = None
    trailing_slash_required: bool = True


RESOURCE_SPECS: tuple[ResourceSpec, ...] = (
    ResourceSpec(
        key="categoria",
        display_name="categorias",
        path="/sistema/api/v1/categoria/",
        response_kind="flat",
        mode="snapshot",
        table_names=("categorias",),
    ),
    ResourceSpec(
        key="bodega",
        display_name="bodegas",
        path="/sistema/api/v1/bodega/",
        response_kind="flat",
        mode="snapshot",
        table_names=("bodegas",),
    ),
    ResourceSpec(
        key="marca",
        display_name="marcas",
        path="/sistema/api/v1/marca/",
        response_kind="flat",
        mode="snapshot",
        table_names=("marcas",),
    ),
    ResourceSpec(
        key="cuenta-contable",
        display_name="cuentas_contables",
        path="/sistema/api/v1/contabilidad/cuenta-contable/",
        response_kind="flat",
        mode="snapshot",
        table_names=("cuentas_contables",),
    ),
    ResourceSpec(
        key="centro-costo",
        display_name="centros_costo",
        path="/sistema/api/v1/contabilidad/centro-costo/",
        response_kind="flat",
        mode="snapshot",
        table_names=("centros_costo",),
    ),
    ResourceSpec(
        key="unidad",
        display_name="unidades",
        path="/sistema/api/v2/unidad/",
        response_kind="paginated",
        mode="snapshot",
        table_names=("unidades",),
    ),
    ResourceSpec(
        key="persona",
        display_name="personas",
        path="/sistema/api/v2/persona/",
        response_kind="paginated",
        mode="incremental",
        table_names=("personas",),
        date_format=ISO_DATE,
        record_date_field="fecha_modificacion",
        record_date_format="iso_prefix",
    ),
    ResourceSpec(
        key="producto",
        display_name="productos",
        path="/sistema/api/v2/producto/",
        response_kind="paginated",
        mode="incremental",
        table_names=("productos",),
        date_format=ISO_DATE,
        record_date_field="fecha_creacion",
        record_date_format="iso_prefix",
    ),
    ResourceSpec(
        key="movimiento-inventario",
        display_name="movimientos",
        path="/sistema/api/v2/movimiento-inventario/",
        response_kind="paginated",
        mode="incremental",
        table_names=("movimientos", "movimiento_detalles"),
        date_format=ISO_DATE,
        record_date_field="fecha",
        record_date_format=EU_DATE,
    ),
    ResourceSpec(
        key="documento",
        display_name="documentos",
        path="/sistema/api/v2/documento/",
        response_kind="paginated",
        mode="incremental",
        table_names=("documentos", "documento_detalles", "documento_cobros"),
        date_format=ISO_DATE,
        record_date_field="fecha_emision",
        record_date_format=EU_DATE,
    ),
    ResourceSpec(
        key="documento/tickets",
        display_name="tickets",
        path="/sistema/api/v2/documento/tickets/",
        response_kind="paginated",
        mode="tickets",
        table_names=("tickets_documentos", "tickets_detalles", "tickets_items"),
    ),
    ResourceSpec(
        key="contabilidad/periodo",
        display_name="periodos",
        path="/sistema/api/v2/contabilidad/periodo/",
        response_kind="paginated",
        mode="snapshot",
        table_names=("periodos",),
    ),
    ResourceSpec(
        key="contabilidad/asiento",
        display_name="asientos",
        path="/sistema/api/v2/contabilidad/asiento/",
        response_kind="paginated",
        mode="incremental",
        table_names=("asientos", "asiento_detalles"),
        date_format=EU_DATE,
        date_start_param="fecha_inicial",
        date_end_param="fecha_final",
        record_date_field="fecha",
        record_date_format=EU_DATE,
    ),
)


def iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def parse_iso_date(value: str) -> dt.date:
    return dt.datetime.strptime(value, ISO_DATE).date()


def format_date(value: dt.date, fmt: str) -> str:
    return value.strftime(fmt)


def parse_record_date(value: str, fmt: str) -> dt.date | None:
    if not value:
        return None
    try:
        if fmt == "iso_prefix":
            return dt.date.fromisoformat(value[:10])
        return dt.datetime.strptime(value, fmt).date()
    except Exception:
        return None


def json_safe(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def row_with_metadata(row: dict[str, Any], run_id: str, ingested_at: str) -> dict[str, str]:
    out = {k: json_safe(v) for k, v in row.items()}
    out["run_id"] = run_id
    out["ingested_at"] = ingested_at
    return out


def key_tuple(row: dict[str, str], keys: list[str]) -> tuple[str, ...]:
    return tuple(row.get(key, "") for key in keys)


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as fh:
        return list(csv.DictReader(fh))


def write_csv_rows(path: Path, rows: list[dict[str, str]], primary_keys: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    seen: set[str] = set()
    for key in primary_keys:
        if key not in seen:
            fieldnames.append(key)
            seen.add(key)
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def merge_rows(
    existing_rows: list[dict[str, str]],
    new_rows: list[dict[str, str]],
    keys: list[str],
) -> list[dict[str, str]]:
    merged: dict[tuple[str, ...], dict[str, str]] = {}
    for row in existing_rows:
        merged[key_tuple(row, keys)] = row
    for row in new_rows:
        merged[key_tuple(row, keys)] = row
    return list(merged.values())


class ApiClient:
    def __init__(
        self,
        base_url: str,
        authorization: str,
        timeout: int = DEFAULT_TIMEOUT,
        retries: int = DEFAULT_RETRIES,
        max_workers: int = DEFAULT_MAX_WORKERS,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.max_workers = max_workers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": authorization,
                "Accept": "application/json",
                "User-Agent": "contifico-extractor/1.0",
            }
        )

    def _absolute_url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        return f"{self.base_url}{path_or_url}"

    def get_json(self, path_or_url: str, params: dict[str, str] | None = None) -> Any:
        url = self._absolute_url(path_or_url)
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except Exception as exc:  # pragma: no cover - network retry
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(1.5 * attempt)
        raise RuntimeError(f"Request failed for {url}: {last_error}") from last_error

    def get_json_or_none(
        self,
        path_or_url: str,
        params: dict[str, str] | None = None,
        *,
        not_found_statuses: tuple[int, ...] = (404,),
    ) -> Any | None:
        url = self._absolute_url(path_or_url)
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                if response.status_code in not_found_statuses or response.status_code == 204:
                    return None
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as exc:  # pragma: no cover - network retry
                last_error = exc
                status_code = exc.response.status_code if exc.response is not None else None
                if status_code in not_found_statuses:
                    return None
                if attempt == self.retries:
                    break
                time.sleep(1.5 * attempt)
            except Exception as exc:  # pragma: no cover - network retry
                last_error = exc
                if attempt == self.retries:
                    break
                time.sleep(1.5 * attempt)
        raise RuntimeError(f"Request failed for {url}: {last_error}") from last_error

    def iter_paginated(
        self,
        path_or_url: str,
        params: dict[str, str] | None = None,
    ) -> Iterable[tuple[int, dict[str, Any]]]:
        next_url: str | None = self._absolute_url(path_or_url)
        current_params = params
        page_number = 1
        while next_url:
            payload = self.get_json(next_url, params=current_params)
            if not isinstance(payload, dict) or "results" not in payload:
                raise RuntimeError(f"Expected paginated payload for {path_or_url}")
            yield page_number, payload
            next_url = payload.get("next")
            current_params = None
            page_number += 1

    def fetch_paginated_pages(
        self,
        path_or_url: str,
        params: dict[str, str] | None = None,
    ) -> list[tuple[int, dict[str, Any]]]:
        first_payload = self.get_json(path_or_url, params=params)
        if not isinstance(first_payload, dict) or "results" not in first_payload:
            raise RuntimeError(f"Expected paginated payload for {path_or_url}")
        first_results = first_payload.get("results", [])
        if not isinstance(first_results, list):
            raise RuntimeError(f"Expected list in paginated payload for {path_or_url}")
        total_count = int(first_payload.get("count", 0) or 0)
        page_size = len(first_results) or 100
        total_pages = max(1, ceil(total_count / page_size))
        pages: list[tuple[int, dict[str, Any]]] = [(1, first_payload)]
        if total_pages <= 1:
            return pages
        worker_count = min(self.max_workers, max(1, total_pages - 1))

        def fetch_page(page_number: int) -> tuple[int, dict[str, Any]]:
            page_params = dict(params or {})
            page_params["page"] = str(page_number)
            payload = self.get_json(path_or_url, params=page_params)
            return page_number, payload

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(fetch_page, page_number) for page_number in range(2, total_pages + 1)]
            for future in as_completed(futures):
                pages.append(future.result())
        pages.sort(key=lambda item: item[0])
        return pages


class CsvStore:
    def __init__(self, output_dir: Path) -> None:
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def table_path(self, table_name: str) -> Path:
        return self.output_dir / f"{table_name}.csv"

    def raw_path(self, run_id: str, resource: str, page_name: str) -> Path:
        safe_resource = resource.replace("/", "_")
        return self.output_dir / "raw" / run_id / safe_resource / f"{page_name}.json"

    def load_rows(self, table_name: str) -> list[dict[str, str]]:
        return load_csv_rows(self.table_path(table_name))

    def save_rows(self, table_name: str, rows: list[dict[str, str]]) -> None:
        primary = PRIMARY_KEYS[table_name]
        write_csv_rows(self.table_path(table_name), rows, primary)

    def merge_or_replace(
        self,
        table_name: str,
        new_rows: list[dict[str, str]],
        replace: bool,
    ) -> int:
        existing = [] if replace else self.load_rows(table_name)
        rows = new_rows if replace else merge_rows(existing, new_rows, PRIMARY_KEYS[table_name])
        self.save_rows(table_name, rows)
        return len(rows)

    def save_raw(self, run_id: str, resource: str, page_name: str, payload: Any) -> None:
        target = self.raw_path(run_id, resource, page_name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("w", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False, indent=2)

    def export_xlsx(self) -> Path | None:
        if Workbook is None:
            return None
        workbook = Workbook()
        default_sheet = workbook.active
        workbook.remove(default_sheet)
        csv_paths = sorted(
            path for path in self.output_dir.glob("*.csv") if path.name != "dataset.xlsx"
        )
        for csv_path in csv_paths:
            sheet_name = csv_path.stem[:31]
            worksheet = workbook.create_sheet(title=sheet_name)
            with csv_path.open("r", encoding="utf-8-sig", newline="") as fh:
                reader = csv.reader(fh)
                for row in reader:
                    worksheet.append(row)
        xlsx_path = self.output_dir / "dataset.xlsx"
        workbook.save(xlsx_path)
        return xlsx_path


def build_window_params(spec: ResourceSpec, from_date: dt.date, to_date: dt.date) -> dict[str, str]:
    if not spec.date_format:
        return {}
    return {
        spec.date_start_param: format_date(from_date, spec.date_format),
        spec.date_end_param: format_date(to_date, spec.date_format),
    }


def extract_simple_table(
    table_name: str,
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: str,
) -> dict[str, list[dict[str, str]]]:
    return {table_name: [row_with_metadata(record, run_id, ingested_at) for record in records]}


def extract_movimientos(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: str,
) -> dict[str, list[dict[str, str]]]:
    masters: list[dict[str, str]] = []
    details: list[dict[str, str]] = []
    for record in records:
        base = dict(record)
        nested = base.pop("detalles", []) or []
        masters.append(row_with_metadata(base, run_id, ingested_at))
        for index, detail in enumerate(nested):
            detail_row = dict(detail)
            detail_row["movimiento_id"] = json_safe(base.get("id"))
            detail_row["detalle_index"] = str(index)
            details.append(row_with_metadata(detail_row, run_id, ingested_at))
    return {"movimientos": masters, "movimiento_detalles": details}


def extract_documentos(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: str,
) -> dict[str, list[dict[str, str]]]:
    masters: list[dict[str, str]] = []
    details: list[dict[str, str]] = []
    cobros: list[dict[str, str]] = []
    for record in records:
        base = dict(record)
        detalles = base.pop("detalles", []) or []
        cobros_payload = base.pop("cobros", []) or []
        cliente = base.pop("cliente", None)
        persona = base.pop("persona", None)
        vendedor = base.pop("vendedor", None)
        if isinstance(cliente, dict):
            base["cliente_id"] = cliente.get("id")
        if isinstance(persona, dict):
            base["persona_obj_id"] = persona.get("id")
        if isinstance(vendedor, dict):
            base["vendedor_obj_id"] = vendedor.get("id")
        masters.append(row_with_metadata(base, run_id, ingested_at))
        documento_id = json_safe(base.get("id"))
        for index, detail in enumerate(detalles):
            detail_row = dict(detail)
            detail_row["documento_id"] = documento_id
            detail_row["detalle_index"] = str(index)
            details.append(row_with_metadata(detail_row, run_id, ingested_at))
        for index, cobro in enumerate(cobros_payload):
            cobro_row = dict(cobro)
            cobro_row["documento_id"] = documento_id
            cobro_row["cobro_index"] = str(index)
            cobros.append(row_with_metadata(cobro_row, run_id, ingested_at))
    return {
        "documentos": masters,
        "documento_detalles": details,
        "documento_cobros": cobros,
    }


def extract_tickets(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: str,
) -> dict[str, list[dict[str, str]]]:
    docs: list[dict[str, str]] = []
    details: list[dict[str, str]] = []
    items: list[dict[str, str]] = []
    for record in records:
        base = dict(record)
        nested_details = base.pop("detalles", []) or []
        docs.append(row_with_metadata(base, run_id, ingested_at))
        documento_id = json_safe(base.get("id"))
        for detail_index, detail in enumerate(nested_details):
            detail_row = dict(detail)
            ticket_items = detail_row.pop("tickets", []) or []
            detail_row["documento_id"] = documento_id
            detail_row["detalle_index"] = str(detail_index)
            details.append(row_with_metadata(detail_row, run_id, ingested_at))
            for ticket_index, ticket in enumerate(ticket_items):
                item_row = dict(ticket)
                item_row["documento_id"] = documento_id
                item_row["detalle_index"] = str(detail_index)
                item_row["ticket_index"] = str(ticket_index)
                items.append(row_with_metadata(item_row, run_id, ingested_at))
    return {
        "tickets_documentos": docs,
        "tickets_detalles": details,
        "tickets_items": items,
    }


def extract_asientos(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: str,
) -> dict[str, list[dict[str, str]]]:
    masters: list[dict[str, str]] = []
    details: list[dict[str, str]] = []
    for record in records:
        base = dict(record)
        nested = base.pop("detalles", []) or []
        masters.append(row_with_metadata(base, run_id, ingested_at))
        asiento_id = json_safe(base.get("id"))
        for index, detail in enumerate(nested):
            detail_row = dict(detail)
            detail_row["asiento_id"] = asiento_id
            detail_row["detalle_index"] = str(index)
            details.append(row_with_metadata(detail_row, run_id, ingested_at))
    return {"asientos": masters, "asiento_detalles": details}


def dispatch_extract(
    spec: ResourceSpec,
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: str,
) -> dict[str, list[dict[str, str]]]:
    if spec.key == "movimiento-inventario":
        return extract_movimientos(records, run_id, ingested_at)
    if spec.key == "documento":
        return extract_documentos(records, run_id, ingested_at)
    if spec.key == "documento/tickets":
        return extract_tickets(records, run_id, ingested_at)
    if spec.key == "contabilidad/asiento":
        return extract_asientos(records, run_id, ingested_at)
    return extract_simple_table(spec.table_names[0], records, run_id, ingested_at)


def read_watermarks(store: CsvStore) -> dict[str, dict[str, str]]:
    rows = store.load_rows("watermarks")
    return {row["resource"]: row for row in rows if row.get("resource")}


def resolve_window(
    spec: ResourceSpec,
    mode: str,
    from_date: dt.date | None,
    to_date: dt.date | None,
    overlap_days: int,
    watermarks: dict[str, dict[str, str]],
) -> tuple[dt.date, dt.date] | None:
    if mode == "backfill" or spec.mode != "incremental":
        return None
    if from_date and to_date:
        return from_date, to_date
    if from_date and not to_date:
        return from_date, dt.date.today()
    watermark = watermarks.get(spec.key)
    if not watermark:
        raise ValueError(
            f"Incremental for '{spec.key}' requires --from-date/--to-date or an existing watermark"
        )
    watermark_to = parse_iso_date(watermark["last_successful_to"])
    effective_from = watermark_to - dt.timedelta(days=overlap_days)
    effective_to = to_date or dt.date.today()
    return effective_from, effective_to


def prepare_requests(
    client: ApiClient,
    spec: ResourceSpec,
    mode: str,
    window: tuple[dt.date, dt.date] | None,
    changed_document_ids: list[str] | None,
    save_raw: bool,
    store: CsvStore,
    run_id: str,
) -> tuple[list[dict[str, Any]], int, int]:
    records: list[dict[str, Any]] = []
    pages_fetched = 0
    reported_total = 0
    if spec.mode == "tickets" and mode == "incremental":
        ids = changed_document_ids or []
        for index, document_id in enumerate(ids, start=1):
            payload = client.get_json(f"{spec.path}{document_id}/")
            pages_fetched += 1
            records.append(payload)
            if save_raw:
                store.save_raw(run_id, spec.key, f"doc_{index:05d}", payload)
        return records, len(records), pages_fetched

    if spec.response_kind == "flat":
        payload = client.get_json(spec.path)
        if not isinstance(payload, list):
            raise RuntimeError(f"Expected flat list response for {spec.key}")
        if save_raw:
            store.save_raw(run_id, spec.key, "snapshot", payload)
        return payload, len(payload), 1

    params = None
    if window:
        params = build_window_params(spec, window[0], window[1])
    for page_number, payload in client.fetch_paginated_pages(spec.path, params=params):
        page_rows = payload.get("results", [])
        if not isinstance(page_rows, list):
            raise RuntimeError(f"Expected results list for {spec.key}")
        if page_number == 1:
            reported_total = int(payload.get("count", 0) or 0)
        records.extend(page_rows)
        pages_fetched += 1
        if save_raw:
            store.save_raw(run_id, spec.key, f"page_{page_number:05d}", payload)
    if window and spec.record_date_field and spec.record_date_format:
        filtered: list[dict[str, Any]] = []
        for record in records:
            record_date = parse_record_date(
                json_safe(record.get(spec.record_date_field)),
                spec.record_date_format,
            )
            if record_date and window[0] <= record_date <= window[1]:
                filtered.append(record)
        records = filtered
    return records, len(records) if window else reported_total, pages_fetched


def save_watermark(
    store: CsvStore,
    resource: str,
    mode: str,
    run_id: str,
    started_at: str,
    window: tuple[dt.date, dt.date] | None,
    replace: bool,
) -> None:
    rows = [] if replace else store.load_rows("watermarks")
    new_row = {
        "resource": resource,
        "mode": mode,
        "last_run_id": run_id,
        "last_successful_from": format_date(window[0], ISO_DATE) if window else "",
        "last_successful_to": format_date(window[1], ISO_DATE) if window else "",
        "updated_at": started_at,
    }
    merged = merge_rows(rows, [new_row], PRIMARY_KEYS["watermarks"])
    store.save_rows("watermarks", merged)


def append_extract_run(
    store: CsvStore,
    run_row: dict[str, str],
) -> None:
    rows = store.load_rows("extract_runs")
    rows = merge_rows(rows, [run_row], PRIMARY_KEYS["extract_runs"])
    store.save_rows("extract_runs", rows)


def build_run_row(
    run_id: str,
    resource: str,
    mode: str,
    status: str,
    started_at: str,
    finished_at: str,
    source_count: int,
    pages_fetched: int,
    table_counts: dict[str, int],
    window: tuple[dt.date, dt.date] | None,
    error: str = "",
) -> dict[str, str]:
    return {
        "run_id": run_id,
        "resource": resource,
        "mode": mode,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "source_count": str(source_count),
        "pages_fetched": str(pages_fetched),
        "table_counts_json": json.dumps(table_counts, ensure_ascii=False, sort_keys=True),
        "from_date": format_date(window[0], ISO_DATE) if window else "",
        "to_date": format_date(window[1], ISO_DATE) if window else "",
        "error": error,
    }


def print_progress(message: str) -> None:
    print(message, flush=True)


def validate_status(client: ApiClient) -> None:
    checks = [
        "/sistema/api/v2/persona/?page=1",
        "/sistema/api/v2/producto/?page=1",
        "/sistema/api/v2/documento/?page=1",
        "/sistema/api/v2/movimiento-inventario/?page=1",
        "/sistema/api/v2/documento/tickets/?page=1",
        "/sistema/api/v2/contabilidad/asiento/?page=1",
        "/sistema/api/v2/contabilidad/periodo/?page=1",
        "/sistema/api/v2/unidad/?page=1",
        "/sistema/api/v1/categoria/",
        "/sistema/api/v1/bodega/",
        "/sistema/api/v1/marca/",
        "/sistema/api/v1/contabilidad/cuenta-contable/",
        "/sistema/api/v1/contabilidad/centro-costo/",
    ]
    for path in checks:
        client.get_json(path)


def run_extraction(args: argparse.Namespace) -> int:
    authorization = os.getenv("CONTIFICO_AUTHORIZATION")
    if not authorization:
        raise RuntimeError("Missing CONTIFICO_AUTHORIZATION environment variable")

    output_dir = Path(args.output_dir).resolve()
    store = CsvStore(output_dir)
    client = ApiClient(args.base_url, authorization)
    run_id = uuid.uuid4().hex
    ingested_at = iso_now()
    from_date = parse_iso_date(args.from_date) if args.from_date else None
    to_date = parse_iso_date(args.to_date) if args.to_date else None
    watermarks = read_watermarks(store)

    print_progress("Validating API access...")
    validate_status(client)

    changed_document_ids: list[str] = []

    for spec in RESOURCE_SPECS:
        resource_started = iso_now()
        print_progress(f"Extracting {spec.key}...")
        replace_tables = args.mode == "backfill" or spec.mode == "snapshot"
        window = resolve_window(
            spec,
            args.mode,
            from_date,
            to_date,
            args.overlap_days,
            watermarks,
        )
        try:
            records, source_count, pages_fetched = prepare_requests(
                client=client,
                spec=spec,
                mode=args.mode,
                window=window,
                changed_document_ids=changed_document_ids,
                save_raw=args.save_raw,
                store=store,
                run_id=run_id,
            )
            extracted_tables = dispatch_extract(spec, records, run_id, ingested_at)
            table_counts: dict[str, int] = {}
            for table_name, table_rows in extracted_tables.items():
                final_count = store.merge_or_replace(
                    table_name=table_name,
                    new_rows=table_rows,
                    replace=replace_tables,
                )
                table_counts[table_name] = final_count
            if spec.key == "documento":
                changed_document_ids = [row.get("id", "") for row in extracted_tables["documentos"] if row.get("id")]
            if spec.mode in {"incremental", "snapshot", "tickets"}:
                save_watermark(
                    store=store,
                    resource=spec.key,
                    mode=args.mode,
                    run_id=run_id,
                    started_at=resource_started,
                    window=window,
                    replace=False,
                )
            append_extract_run(
                store,
                build_run_row(
                    run_id=run_id,
                    resource=spec.key,
                    mode=args.mode,
                    status="success",
                    started_at=resource_started,
                    finished_at=iso_now(),
                    source_count=source_count,
                    pages_fetched=pages_fetched,
                    table_counts=table_counts,
                    window=window,
                ),
            )
            print_progress(
                f"Done {spec.key}: source_rows={len(records)} pages={pages_fetched} tables={table_counts}"
            )
        except Exception as exc:
            append_extract_run(
                store,
                build_run_row(
                    run_id=run_id,
                    resource=spec.key,
                    mode=args.mode,
                    status="failed",
                    started_at=resource_started,
                    finished_at=iso_now(),
                    source_count=0,
                    pages_fetched=0,
                    table_counts={},
                    window=window,
                    error=str(exc),
                ),
            )
            raise

    if args.export_xlsx:
        xlsx_path = store.export_xlsx()
        if xlsx_path:
            print_progress(f"Excel export created at {xlsx_path}")
        else:
            print_progress("Excel export skipped because openpyxl is not available")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Contifico V2 live dataset extractor")
    parser.add_argument("--mode", choices=("backfill", "incremental"), required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--from-date", help="ISO date YYYY-MM-DD")
    parser.add_argument("--to-date", help="ISO date YYYY-MM-DD")
    parser.add_argument("--overlap-days", type=int, default=DEFAULT_OVERLAP_DAYS)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--save-raw", action="store_true")
    parser.add_argument("--export-xlsx", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_extraction(args)


if __name__ == "__main__":
    sys.exit(main())
