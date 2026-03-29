from __future__ import annotations

import argparse
import datetime as dt
import io
import os
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import psycopg2

from contifico_pg_backfill import (
    DEFAULT_DB_NAME,
    create_reporting_views,
    ensure_schema,
    open_connection,
    pg_config_from_env,
    validate_post_load_constraints,
)


SYNC_TABLE_ORDER = [
    "core.cuentas_contables",
    "core.categorias",
    "core.bodegas",
    "core.marcas",
    "core.unidades",
    "core.centros_costo",
    "core.periodos",
    "core.banco_cuentas",
    "core.personas",
    "core.productos",
    "core.guias",
    "core.banco_movimientos",
    "core.banco_movimiento_detalles",
    "core.movimientos",
    "core.movimiento_detalles",
    "core.documentos",
    "core.documento_detalles",
    "core.documento_cobros",
    "core.guia_destinatarios",
    "core.guia_detalles",
    "core.tickets_documentos",
    "core.tickets_detalles",
    "core.tickets_items",
    "core.asientos",
    "core.asiento_detalles",
    "meta.extract_runs",
    "meta.load_metrics",
    "meta.watermarks",
]

RAW_TABLES = ["raw.resource_rows"]


def normalize_target_dsn(dsn: str) -> str:
    parsed = urlparse(dsn)
    if parsed.scheme in {"postgres", "postgresql"}:
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        query.setdefault("sslmode", "require")
        return urlunparse(parsed._replace(query=urlencode(query)))
    if "sslmode=" not in dsn:
        return f"{dsn} sslmode=require"
    return dsn


def target_dsn_from_env(explicit: str | None) -> str:
    dsn = explicit or os.getenv("SUPABASE_DB_URL") or os.getenv("SUPABASE_DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing Supabase target connection. Use --target-dsn or SUPABASE_DB_URL")
    return normalize_target_dsn(dsn)


def connect_target(dsn: str):
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    return conn


def ensure_schema_target(conn) -> None:
    ensure_schema(conn)


def truncate_target(conn, include_raw: bool) -> None:
    tables = [
        "meta.watermarks",
        "meta.load_metrics",
        "meta.extract_runs",
        "core.tickets_items",
        "core.tickets_detalles",
        "core.tickets_documentos",
        "core.documento_cobros",
        "core.documento_detalles",
        "core.documentos",
        "core.asiento_detalles",
        "core.asientos",
        "core.movimiento_detalles",
        "core.movimientos",
        "core.banco_movimiento_detalles",
        "core.banco_movimientos",
        "core.guia_detalles",
        "core.guia_destinatarios",
        "core.guias",
        "core.productos",
        "core.personas",
        "core.banco_cuentas",
        "core.periodos",
        "core.centros_costo",
        "core.unidades",
        "core.marcas",
        "core.bodegas",
        "core.categorias",
        "core.cuentas_contables",
    ]
    if include_raw:
        tables.insert(0, "raw.resource_rows")
    statement = "TRUNCATE TABLE " + ", ".join(tables) + " CASCADE"
    with conn.cursor() as cur:
        cur.execute(statement)
    conn.commit()


def count_rows(conn, qualified_table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*)::bigint FROM {qualified_table}")
        return int(cur.fetchone()[0] or 0)


def copy_table(source_conn, target_conn, qualified_table: str) -> tuple[int, int]:
    source_count = count_rows(source_conn, qualified_table)
    if source_count > 0:
        buffer = io.StringIO()
        with source_conn.cursor() as src_cur:
            src_cur.copy_expert(f"COPY {qualified_table} TO STDOUT WITH CSV HEADER", buffer)
        buffer.seek(0)
        with target_conn.cursor() as dst_cur:
            dst_cur.copy_expert(f"COPY {qualified_table} FROM STDIN WITH CSV HEADER", buffer)
    target_count = count_rows(target_conn, qualified_table)
    return source_count, target_count


def write_report(path: Path, payload: dict[str, Any]) -> None:
    lines = [
        "# Sync Report",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Source DB: `{payload['source_db']}`",
        f"- Target: `{payload['target_label']}`",
        f"- Included raw: `{'si' if payload['include_raw'] else 'no'}`",
        "",
        "## Tables",
        "",
        "| Table | Source rows | Target rows | Status |",
        "| --- | --- | --- | --- |",
    ]
    for row in payload["tables"]:
        lines.append(f"| {row['table_name']} | {row['source_rows']} | {row['target_rows']} | {row['status']} |")
    path.write_text("\n".join(lines), encoding="utf-8")


def run_sync(args: argparse.Namespace) -> int:
    source_config = pg_config_from_env(args.source_db_name)
    target_dsn = target_dsn_from_env(args.target_dsn)
    tables = list(SYNC_TABLE_ORDER)
    if args.include_raw:
        tables.extend(RAW_TABLES)
    report_rows: list[dict[str, Any]] = []
    with open_connection(source_config, source_config.db_name) as source_conn, connect_target(target_dsn) as target_conn:
        ensure_schema_target(target_conn)
        if args.truncate_target:
            truncate_target(target_conn, include_raw=args.include_raw)
        for qualified_table in tables:
            source_rows, target_rows = copy_table(source_conn, target_conn, qualified_table)
            report_rows.append(
                {
                    "table_name": qualified_table,
                    "source_rows": source_rows,
                    "target_rows": target_rows,
                    "status": "ok" if source_rows == target_rows else "mismatch",
                }
            )
            target_conn.commit()
        validate_post_load_constraints(target_conn)
        create_reporting_views(target_conn)
        report_payload = {
            "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "source_db": args.source_db_name,
            "target_label": urlparse(target_dsn).netloc or "supabase-target",
            "include_raw": args.include_raw,
            "tables": report_rows,
        }
        write_report(Path(args.report_out).resolve(), report_payload)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Sync Contifico PostgreSQL model into Supabase Postgres")
    parser.add_argument("--source-db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--target-dsn")
    parser.add_argument("--include-raw", action="store_true")
    parser.add_argument("--no-truncate-target", action="store_true")
    parser.add_argument("--report-out", default="supabase_sync_report.md")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.truncate_target = not args.no_truncate_target
    return run_sync(args)


if __name__ == "__main__":
    raise SystemExit(main())
