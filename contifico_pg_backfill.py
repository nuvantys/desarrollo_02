from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json, execute_values

from contifico_extractor import (
    DEFAULT_BASE_URL,
    DEFAULT_MAX_WORKERS,
    ApiClient,
    RESOURCE_SPECS,
    ResourceSpec,
    iso_now,
    validate_status,
)


APP_TZ = ZoneInfo("America/Guayaquil")
UTC = dt.timezone.utc
DEFAULT_DB_NAME = "contifico_backfill"
DEFAULT_PGHOST = "127.0.0.1"
DEFAULT_PGPORT = 5432
DEFAULT_PG_MAINTENANCE_DB = "postgres"
BACKFILL_MODE = "backfill"


RESOURCE_ORDER = (
    "cuenta-contable",
    "categoria",
    "bodega",
    "marca",
    "unidad",
    "centro-costo",
    "contabilidad/periodo",
    "persona",
    "producto",
    "movimiento-inventario",
    "documento",
    "documento/tickets",
    "contabilidad/asiento",
)


RESOURCE_SPECS_BY_KEY = {spec.key: spec for spec in RESOURCE_SPECS}


CATALOG_RESOURCES = {
    "cuenta-contable",
    "categoria",
    "bodega",
    "marca",
    "unidad",
    "centro-costo",
    "contabilidad/periodo",
}


@dataclass(frozen=True)
class PgConfig:
    host: str
    port: int
    user: str
    password: str
    maintenance_db: str
    db_name: str


def print_progress(message: str) -> None:
    print(message, flush=True)


def to_nonempty_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def to_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in {"true", "t", "1", "yes", "y", "si", "s"}:
        return True
    if text in {"false", "f", "0", "no", "n"}:
        return False
    return None


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(str(value).strip())
    except ValueError:
        try:
            return int(Decimal(str(value).strip()))
        except (InvalidOperation, ValueError):
            return None


def to_decimal(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return Decimal(int(value))
    try:
        return Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def parse_date(value: Any) -> dt.date | None:
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    text = str(value).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return dt.datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    try:
        return dt.date.fromisoformat(text[:10])
    except ValueError:
        return None


def parse_timestamp(value: Any) -> dt.datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, dt.datetime):
        timestamp = value
    else:
        text = str(value).strip()
        timestamp = None
        try:
            timestamp = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            pass
        if timestamp is None:
            for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y"):
                try:
                    timestamp = dt.datetime.strptime(text, fmt)
                    break
                except ValueError:
                    continue
        if timestamp is None:
            return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=APP_TZ)
    return timestamp.astimezone(UTC)


def parse_iso_timestamp(value: str) -> dt.datetime:
    timestamp = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        return timestamp.replace(tzinfo=UTC)
    return timestamp.astimezone(UTC)


def jsonb_value(value: Any) -> Json | None:
    if value is None:
        return None
    return Json(value, dumps=lambda obj: json.dumps(obj, ensure_ascii=False))


def resource_metadata(run_id: str, ingested_at: dt.datetime) -> dict[str, Any]:
    return {"run_id": run_id, "ingested_at": ingested_at}


def pg_config_from_env(db_name: str) -> PgConfig:
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    if not user:
        raise RuntimeError("Missing PGUSER environment variable")
    if password is None:
        raise RuntimeError("Missing PGPASSWORD environment variable")
    return PgConfig(
        host=os.getenv("PGHOST", DEFAULT_PGHOST),
        port=int(os.getenv("PGPORT", str(DEFAULT_PGPORT))),
        user=user,
        password=password,
        maintenance_db=os.getenv("PGMAINTENANCE_DB", DEFAULT_PG_MAINTENANCE_DB),
        db_name=db_name,
    )


def open_connection(config: PgConfig, database: str, autocommit: bool = False):
    conn = psycopg2.connect(
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        dbname=database,
    )
    conn.autocommit = autocommit
    return conn


def ensure_database_exists(config: PgConfig) -> None:
    conn = open_connection(config, config.maintenance_db, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (config.db_name,))
            if cur.fetchone():
                return
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(config.db_name)))
    finally:
        conn.close()


CORE_TABLE_COLUMNS: dict[str, list[str]] = {
    "categorias": ["id", "nombre", "padre_id", "agrupar", "tipo_producto", "cuenta_venta", "cuenta_compra", "cuenta_inventario", "run_id", "ingested_at"],
    "bodegas": ["id", "codigo", "nombre", "venta", "compra", "produccion", "run_id", "ingested_at"],
    "marcas": ["id", "nombre", "run_id", "ingested_at"],
    "unidades": ["id", "nombre", "run_id", "ingested_at"],
    "cuentas_contables": ["id", "nombre", "codigo", "tipo", "run_id", "ingested_at"],
    "centros_costo": ["id", "nombre", "codigo", "tipo", "padre_id", "estado", "run_id", "ingested_at"],
    "periodos": ["id", "fecha_inicio", "fecha_fin", "estado", "dia_cierre_mensual", "run_id", "ingested_at"],
    "personas": [
        "id", "tipo", "personaasociada_id", "es_cliente", "es_proveedor", "es_extranjero", "es_vendedor", "es_empleado",
        "es_corporativo", "ruc", "cedula", "placa", "razon_social", "nombre_comercial", "email", "telefonos", "direccion",
        "pvp_default", "porcentaje_descuento", "adicional1_cliente", "adicional2_cliente", "adicional3_cliente", "adicional4_cliente",
        "adicional1_proveedor", "adicional2_proveedor", "adicional3_proveedor", "adicional4_proveedor", "banco_codigo_id", "tipo_cuenta",
        "numero_tarjeta", "aplicar_cupo", "cuenta_por_cobrar_id", "cuenta_por_pagar_id", "categoria_id", "categoria_nombre",
        "fecha_modificacion", "sueldo", "dias_credito", "cupo_credito", "vendedor_asignado_id", "vendedor_asignado_jsonb", "run_id", "ingested_at",
    ],
    "productos": [
        "id", "unidad_id", "categoria_id", "codigo", "nombre", "codigo_auxiliar", "pvp_manual", "pvp1", "pvp2", "pvp3", "pvp4",
        "generacion_automatica", "porcentaje_iva", "minimo", "estado", "tipo", "tipo_producto", "para_pos", "personalizado1",
        "personalizado2", "descripcion", "codigo_barra", "fecha_creacion", "costo_maximo", "codigo_proveedor", "lead_time",
        "cantidad_stock", "cuenta_venta_id", "cuenta_compra_id", "cuenta_costo_id", "marca_id", "marca_nombre", "imagen",
        "producto_base_id", "nombre_producto_base", "detalle_variantes_jsonb", "codigo_sap", "para_supereasy", "pvp_supereasy",
        "para_comisariato", "pvp_comisariato", "categoria_comisariato", "id_integracion_proveedor", "departamento",
        "descripcion_departamento", "familia", "descripcion_familia", "jerarquia", "descripcion_jerarquia", "indicador_peso",
        "pvp_peso", "peso_desde", "peso_hasta", "porcentaje_ice", "valor_ice", "campo_catalogo", "maneja_nombremanual",
        "porcentaje_servicio", "run_id", "ingested_at",
    ],
    "movimientos": ["id", "codigo", "bodega_id", "tipo", "fecha", "generar_asiento", "pos", "cuenta_id", "maneja_venta", "descripcion", "total", "estado", "bodega_destino_id", "codigo_interno", "proyecto", "run_id", "ingested_at"],
    "movimiento_detalles": ["movimiento_id", "detalle_index", "serie", "producto_id", "edicion", "precio", "cantidad", "unidad_id", "costo_promedio", "run_id", "ingested_at"],
    "documentos": [
        "id", "pos", "persona_id", "cliente_id", "proveedor_id", "vendedor_id", "fecha_emision", "hora_emision", "tipo_registro",
        "tipo_documento", "documento", "electronico", "autorizacion", "estado", "subtotal_12", "subtotal_0", "iva", "ice", "servicio",
        "total", "reserva_relacionada", "descripcion", "referencia", "adicional1", "adicional2", "tarjeta_consumo_id", "logistica",
        "tipo_domicilio", "orden_domicilio_id", "url_ride", "tipo_descuento", "url_xml", "placa", "vendedor_identificacion",
        "fecha_creacion", "saldo_anticipo", "fecha_evento", "hora_evento", "direccion_evento", "pax", "fecha_vencimiento",
        "documento_relacionado_id", "firmado", "saldo", "entregado", "anulado", "caja_id", "fecha_modificacion", "subtotal",
        "autorizado_sri", "enviado_sri", "correo_enviado", "retencion_autorizado_sri", "retencion_firmado", "retencion_enviado_sri",
        "retencion_correo_enviado", "run_id", "ingested_at",
    ],
    "documento_detalles": [
        "documento_id", "detalle_index", "producto_id", "cuenta_id", "centro_costo_id", "base_cero", "base_no_gravable", "base_gravable",
        "cantidad", "codigo_bien", "codigo_imp_iva", "codigo_imp_ret", "descripcion", "documento_ref", "formula_jsonb", "ibpnr",
        "nombre_manual", "peso", "porcentaje_descuento", "porcentaje_ice", "porcentaje_iva", "precio", "producto_descripcion",
        "producto_nombre", "promocion_integracion_id", "serie", "valor_ice", "volumen", "run_id", "ingested_at",
    ],
    "documento_cobros": [
        "documento_id", "cobro_index", "forma_cobro", "numero_comprobante", "caja_id", "monto", "numero_tarjeta", "fecha",
        "fecha_creacion", "nombre_tarjeta", "tipo_banco", "bin_tarjeta", "cuenta_bancaria_id", "monto_propina", "numero_cheque",
        "fecha_cheque", "tipo_ping", "lote", "run_id", "ingested_at",
    ],
    "tickets_documentos": ["id", "fecha_emision", "run_id", "ingested_at"],
    "tickets_detalles": ["documento_id", "detalle_index", "producto_id", "centro_costo_id", "producto_nombre", "descripcion", "vendidos", "leidos", "run_id", "ingested_at"],
    "tickets_items": ["documento_id", "detalle_index", "ticket_index", "payload_jsonb", "run_id", "ingested_at"],
    "asientos": ["id", "glosa", "fecha", "run_id", "ingested_at"],
    "asiento_detalles": ["asiento_id", "detalle_index", "cuenta_id", "centro_costo_id", "tipo", "valor", "run_id", "ingested_at"],
}

CORE_PRIMARY_KEYS: dict[str, list[str]] = {
    "categorias": ["id"],
    "bodegas": ["id"],
    "marcas": ["id"],
    "unidades": ["id"],
    "cuentas_contables": ["id"],
    "centros_costo": ["id"],
    "periodos": ["id"],
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
}


REFERENCE_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "categorias": [("cuentas_contables", "cuenta_venta"), ("cuentas_contables", "cuenta_compra"), ("cuentas_contables", "cuenta_inventario"), ("categorias", "padre_id")],
    "centros_costo": [("centros_costo", "padre_id")],
    "personas": [("personas", "personaasociada_id"), ("categorias", "categoria_id"), ("cuentas_contables", "cuenta_por_cobrar_id"), ("cuentas_contables", "cuenta_por_pagar_id"), ("personas", "vendedor_asignado_id")],
    "productos": [("unidades", "unidad_id"), ("categorias", "categoria_id"), ("marcas", "marca_id"), ("cuentas_contables", "cuenta_venta_id"), ("cuentas_contables", "cuenta_compra_id"), ("cuentas_contables", "cuenta_costo_id"), ("productos", "producto_base_id")],
    "movimientos": [("bodegas", "bodega_id"), ("bodegas", "bodega_destino_id"), ("cuentas_contables", "cuenta_id")],
    "movimiento_detalles": [("movimientos", "movimiento_id"), ("productos", "producto_id"), ("unidades", "unidad_id")],
    "documentos": [("personas", "persona_id"), ("personas", "cliente_id"), ("personas", "proveedor_id"), ("personas", "vendedor_id"), ("documentos", "documento_relacionado_id")],
    "documento_detalles": [("documentos", "documento_id"), ("productos", "producto_id"), ("cuentas_contables", "cuenta_id"), ("centros_costo", "centro_costo_id")],
    "documento_cobros": [("documentos", "documento_id")],
    "tickets_documentos": [("documentos", "id")],
    "tickets_detalles": [("tickets_documentos", "documento_id"), ("productos", "producto_id"), ("centros_costo", "centro_costo_id")],
    "asiento_detalles": [("asientos", "asiento_id"), ("cuentas_contables", "cuenta_id"), ("centros_costo", "centro_costo_id")],
}


STUB_TARGET_ORDER = [
    "cuentas_contables",
    "categorias",
    "bodegas",
    "marcas",
    "unidades",
    "centros_costo",
    "personas",
    "productos",
    "movimientos",
    "documentos",
    "tickets_documentos",
    "asientos",
]


META_EXTRACT_RUN_COLUMNS = [
    "run_id", "resource", "mode", "status", "started_at", "finished_at", "source_count", "pages_fetched",
    "raw_row_count", "table_counts_jsonb", "error_text", "created_at",
]
META_LOAD_METRIC_COLUMNS = ["run_id", "resource", "stage", "table_name", "row_count", "measured_at"]
META_WATERMARK_COLUMNS = ["resource", "last_run_id", "min_record_date", "max_record_date", "updated_at"]
RAW_RESOURCE_ROW_COLUMNS = ["run_id", "resource", "entity_id", "parent_entity_id", "page_number", "request_params_jsonb", "payload_jsonb", "fetched_at"]


DDL_STATEMENTS = [
    "CREATE SCHEMA IF NOT EXISTS meta",
    "CREATE SCHEMA IF NOT EXISTS raw",
    "CREATE SCHEMA IF NOT EXISTS core",
    "CREATE SCHEMA IF NOT EXISTS reporting",
    """
    CREATE TABLE IF NOT EXISTS meta.extract_runs (
        run_id text NOT NULL,
        resource text NOT NULL,
        mode text NOT NULL,
        status text NOT NULL,
        started_at timestamptz NOT NULL,
        finished_at timestamptz,
        source_count bigint NOT NULL DEFAULT 0,
        pages_fetched integer NOT NULL DEFAULT 0,
        raw_row_count bigint NOT NULL DEFAULT 0,
        table_counts_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
        error_text text,
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (run_id, resource)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS meta.watermarks (
        resource text PRIMARY KEY,
        last_run_id text NOT NULL,
        min_record_date date,
        max_record_date date,
        updated_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS meta.load_metrics (
        run_id text NOT NULL,
        resource text NOT NULL,
        stage text NOT NULL,
        table_name text NOT NULL,
        row_count bigint NOT NULL,
        measured_at timestamptz NOT NULL,
        PRIMARY KEY (run_id, resource, stage, table_name)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.resource_rows (
        run_id text NOT NULL,
        resource text NOT NULL,
        entity_id text NOT NULL,
        parent_entity_id text,
        page_number integer NOT NULL,
        request_params_jsonb jsonb NOT NULL DEFAULT '{}'::jsonb,
        payload_jsonb jsonb NOT NULL,
        fetched_at timestamptz NOT NULL,
        PRIMARY KEY (run_id, resource, entity_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_raw_resource_rows_resource ON raw.resource_rows (resource)",
    "CREATE INDEX IF NOT EXISTS idx_raw_resource_rows_parent ON raw.resource_rows (parent_entity_id)",
    """
    CREATE TABLE IF NOT EXISTS core.cuentas_contables (
        id text PRIMARY KEY,
        nombre text,
        codigo text,
        tipo text,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.categorias (
        id text PRIMARY KEY,
        nombre text,
        padre_id text REFERENCES core.categorias(id) DEFERRABLE INITIALLY DEFERRED,
        agrupar boolean,
        tipo_producto text,
        cuenta_venta text REFERENCES core.cuentas_contables(id),
        cuenta_compra text REFERENCES core.cuentas_contables(id),
        cuenta_inventario text REFERENCES core.cuentas_contables(id),
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.bodegas (
        id text PRIMARY KEY,
        codigo text,
        nombre text,
        venta boolean,
        compra boolean,
        produccion boolean,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.marcas (
        id text PRIMARY KEY,
        nombre text,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.unidades (
        id text PRIMARY KEY,
        nombre text,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.centros_costo (
        id text PRIMARY KEY,
        nombre text,
        codigo text,
        tipo text,
        padre_id text REFERENCES core.centros_costo(id) DEFERRABLE INITIALLY DEFERRED,
        estado text,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.periodos (
        id text PRIMARY KEY,
        fecha_inicio date,
        fecha_fin date,
        estado text,
        dia_cierre_mensual integer,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.personas (
        id text PRIMARY KEY,
        tipo text,
        personaasociada_id text REFERENCES core.personas(id) DEFERRABLE INITIALLY DEFERRED,
        es_cliente boolean,
        es_proveedor boolean,
        es_extranjero boolean,
        es_vendedor boolean,
        es_empleado boolean,
        es_corporativo boolean,
        ruc text,
        cedula text,
        placa text,
        razon_social text,
        nombre_comercial text,
        email text,
        telefonos text,
        direccion text,
        pvp_default text,
        porcentaje_descuento numeric(18,6),
        adicional1_cliente text,
        adicional2_cliente text,
        adicional3_cliente text,
        adicional4_cliente text,
        adicional1_proveedor text,
        adicional2_proveedor text,
        adicional3_proveedor text,
        adicional4_proveedor text,
        banco_codigo_id text,
        tipo_cuenta text,
        numero_tarjeta text,
        aplicar_cupo boolean,
        cuenta_por_cobrar_id text REFERENCES core.cuentas_contables(id),
        cuenta_por_pagar_id text REFERENCES core.cuentas_contables(id),
        categoria_id text REFERENCES core.categorias(id),
        categoria_nombre text,
        fecha_modificacion timestamptz,
        sueldo numeric(18,6),
        dias_credito integer,
        cupo_credito numeric(18,6),
        vendedor_asignado_id text REFERENCES core.personas(id) DEFERRABLE INITIALLY DEFERRED,
        vendedor_asignado_jsonb jsonb,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.productos (
        id text PRIMARY KEY,
        unidad_id text REFERENCES core.unidades(id),
        categoria_id text REFERENCES core.categorias(id),
        codigo text,
        nombre text,
        codigo_auxiliar text,
        pvp_manual boolean,
        pvp1 numeric(18,6),
        pvp2 numeric(18,6),
        pvp3 numeric(18,6),
        pvp4 numeric(18,6),
        generacion_automatica boolean,
        porcentaje_iva numeric(18,6),
        minimo numeric(18,6),
        estado text,
        tipo text,
        tipo_producto text,
        para_pos boolean,
        personalizado1 text,
        personalizado2 text,
        descripcion text,
        codigo_barra text,
        fecha_creacion timestamptz,
        costo_maximo numeric(18,6),
        codigo_proveedor text,
        lead_time integer,
        cantidad_stock numeric(18,6),
        cuenta_venta_id text REFERENCES core.cuentas_contables(id),
        cuenta_compra_id text REFERENCES core.cuentas_contables(id),
        cuenta_costo_id text REFERENCES core.cuentas_contables(id),
        marca_id text REFERENCES core.marcas(id),
        marca_nombre text,
        imagen text,
        producto_base_id text REFERENCES core.productos(id) DEFERRABLE INITIALLY DEFERRED,
        nombre_producto_base text,
        detalle_variantes_jsonb jsonb,
        codigo_sap text,
        para_supereasy boolean,
        pvp_supereasy numeric(18,6),
        para_comisariato boolean,
        pvp_comisariato numeric(18,6),
        categoria_comisariato text,
        id_integracion_proveedor text,
        departamento text,
        descripcion_departamento text,
        familia text,
        descripcion_familia text,
        jerarquia text,
        descripcion_jerarquia text,
        indicador_peso boolean,
        pvp_peso numeric(18,6),
        peso_desde numeric(18,6),
        peso_hasta numeric(18,6),
        porcentaje_ice numeric(18,6),
        valor_ice numeric(18,6),
        campo_catalogo text,
        maneja_nombremanual boolean,
        porcentaje_servicio text,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.movimientos (
        id text PRIMARY KEY,
        codigo text,
        bodega_id text REFERENCES core.bodegas(id),
        tipo text,
        fecha date,
        generar_asiento boolean,
        pos text,
        cuenta_id text REFERENCES core.cuentas_contables(id),
        maneja_venta boolean,
        descripcion text,
        total numeric(18,6),
        estado text,
        bodega_destino_id text REFERENCES core.bodegas(id),
        codigo_interno text,
        proyecto text,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.movimiento_detalles (
        movimiento_id text NOT NULL REFERENCES core.movimientos(id),
        detalle_index integer NOT NULL,
        serie text,
        producto_id text REFERENCES core.productos(id),
        edicion text,
        precio numeric(18,6),
        cantidad numeric(18,6),
        unidad_id text REFERENCES core.unidades(id),
        costo_promedio numeric(18,6),
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL,
        PRIMARY KEY (movimiento_id, detalle_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.documentos (
        id text PRIMARY KEY,
        pos text,
        persona_id text REFERENCES core.personas(id),
        cliente_id text REFERENCES core.personas(id),
        proveedor_id text REFERENCES core.personas(id),
        vendedor_id text REFERENCES core.personas(id),
        fecha_emision date,
        hora_emision text,
        tipo_registro text,
        tipo_documento text,
        documento text,
        electronico boolean,
        autorizacion text,
        estado text,
        subtotal_12 numeric(18,6),
        subtotal_0 numeric(18,6),
        iva numeric(18,6),
        ice numeric(18,6),
        servicio numeric(18,6),
        total numeric(18,6),
        reserva_relacionada text,
        descripcion text,
        referencia text,
        adicional1 text,
        adicional2 text,
        tarjeta_consumo_id text,
        logistica text,
        tipo_domicilio text,
        orden_domicilio_id text,
        url_ride text,
        tipo_descuento text,
        url_xml text,
        placa text,
        vendedor_identificacion text,
        fecha_creacion date,
        saldo_anticipo numeric(18,6),
        fecha_evento date,
        hora_evento text,
        direccion_evento text,
        pax integer,
        fecha_vencimiento date,
        documento_relacionado_id text REFERENCES core.documentos(id) DEFERRABLE INITIALLY DEFERRED,
        firmado boolean,
        saldo numeric(18,6),
        entregado boolean,
        anulado boolean,
        caja_id text,
        fecha_modificacion date,
        subtotal numeric(18,6),
        autorizado_sri boolean,
        enviado_sri boolean,
        correo_enviado boolean,
        retencion_autorizado_sri boolean,
        retencion_firmado boolean,
        retencion_enviado_sri boolean,
        retencion_correo_enviado boolean,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.documento_detalles (
        documento_id text NOT NULL REFERENCES core.documentos(id),
        detalle_index integer NOT NULL,
        producto_id text REFERENCES core.productos(id),
        cuenta_id text REFERENCES core.cuentas_contables(id),
        centro_costo_id text REFERENCES core.centros_costo(id),
        base_cero numeric(18,6),
        base_no_gravable numeric(18,6),
        base_gravable numeric(18,6),
        cantidad numeric(18,6),
        codigo_bien text,
        codigo_imp_iva text,
        codigo_imp_ret text,
        descripcion text,
        documento_ref text,
        formula_jsonb jsonb,
        ibpnr numeric(18,6),
        nombre_manual text,
        peso numeric(18,6),
        porcentaje_descuento numeric(18,6),
        porcentaje_ice numeric(18,6),
        porcentaje_iva numeric(18,6),
        precio numeric(18,6),
        producto_descripcion text,
        producto_nombre text,
        promocion_integracion_id text,
        serie text,
        valor_ice numeric(18,6),
        volumen numeric(18,6),
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL,
        PRIMARY KEY (documento_id, detalle_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.documento_cobros (
        documento_id text NOT NULL REFERENCES core.documentos(id),
        cobro_index integer NOT NULL,
        forma_cobro text,
        numero_comprobante text,
        caja_id text,
        monto numeric(18,6),
        numero_tarjeta text,
        fecha date,
        fecha_creacion timestamptz,
        nombre_tarjeta text,
        tipo_banco text,
        bin_tarjeta text,
        cuenta_bancaria_id text,
        monto_propina numeric(18,6),
        numero_cheque text,
        fecha_cheque date,
        tipo_ping text,
        lote text,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL,
        PRIMARY KEY (documento_id, cobro_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.tickets_documentos (
        id text PRIMARY KEY REFERENCES core.documentos(id),
        fecha_emision date,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.tickets_detalles (
        documento_id text NOT NULL REFERENCES core.tickets_documentos(id),
        detalle_index integer NOT NULL,
        producto_id text REFERENCES core.productos(id),
        centro_costo_id text REFERENCES core.centros_costo(id),
        producto_nombre text,
        descripcion text,
        vendidos integer,
        leidos integer,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL,
        PRIMARY KEY (documento_id, detalle_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.tickets_items (
        documento_id text NOT NULL,
        detalle_index integer NOT NULL,
        ticket_index integer NOT NULL,
        payload_jsonb jsonb NOT NULL,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL,
        PRIMARY KEY (documento_id, detalle_index, ticket_index),
        FOREIGN KEY (documento_id, detalle_index) REFERENCES core.tickets_detalles(documento_id, detalle_index)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.asientos (
        id text PRIMARY KEY,
        glosa text,
        fecha date,
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS core.asiento_detalles (
        asiento_id text NOT NULL REFERENCES core.asientos(id),
        detalle_index integer NOT NULL,
        cuenta_id text REFERENCES core.cuentas_contables(id),
        centro_costo_id text REFERENCES core.centros_costo(id),
        tipo text,
        valor numeric(18,6),
        run_id text NOT NULL,
        ingested_at timestamptz NOT NULL,
        PRIMARY KEY (asiento_id, detalle_index)
    )
    """,
]


FK_HEALTH_CHECKS = [
    ("categorias.padre_id -> categorias.id", "core.categorias", "padre_id", "core.categorias", "id"),
    ("categorias.cuenta_venta -> cuentas_contables.id", "core.categorias", "cuenta_venta", "core.cuentas_contables", "id"),
    ("categorias.cuenta_compra -> cuentas_contables.id", "core.categorias", "cuenta_compra", "core.cuentas_contables", "id"),
    ("categorias.cuenta_inventario -> cuentas_contables.id", "core.categorias", "cuenta_inventario", "core.cuentas_contables", "id"),
    ("centros_costo.padre_id -> centros_costo.id", "core.centros_costo", "padre_id", "core.centros_costo", "id"),
    ("personas.personaasociada_id -> personas.id", "core.personas", "personaasociada_id", "core.personas", "id"),
    ("personas.categoria_id -> categorias.id", "core.personas", "categoria_id", "core.categorias", "id"),
    ("personas.cuenta_por_cobrar_id -> cuentas_contables.id", "core.personas", "cuenta_por_cobrar_id", "core.cuentas_contables", "id"),
    ("personas.cuenta_por_pagar_id -> cuentas_contables.id", "core.personas", "cuenta_por_pagar_id", "core.cuentas_contables", "id"),
    ("personas.vendedor_asignado_id -> personas.id", "core.personas", "vendedor_asignado_id", "core.personas", "id"),
    ("productos.unidad_id -> unidades.id", "core.productos", "unidad_id", "core.unidades", "id"),
    ("productos.categoria_id -> categorias.id", "core.productos", "categoria_id", "core.categorias", "id"),
    ("productos.marca_id -> marcas.id", "core.productos", "marca_id", "core.marcas", "id"),
    ("productos.cuenta_venta_id -> cuentas_contables.id", "core.productos", "cuenta_venta_id", "core.cuentas_contables", "id"),
    ("productos.cuenta_compra_id -> cuentas_contables.id", "core.productos", "cuenta_compra_id", "core.cuentas_contables", "id"),
    ("productos.cuenta_costo_id -> cuentas_contables.id", "core.productos", "cuenta_costo_id", "core.cuentas_contables", "id"),
    ("productos.producto_base_id -> productos.id", "core.productos", "producto_base_id", "core.productos", "id"),
    ("movimientos.bodega_id -> bodegas.id", "core.movimientos", "bodega_id", "core.bodegas", "id"),
    ("movimientos.bodega_destino_id -> bodegas.id", "core.movimientos", "bodega_destino_id", "core.bodegas", "id"),
    ("movimientos.cuenta_id -> cuentas_contables.id", "core.movimientos", "cuenta_id", "core.cuentas_contables", "id"),
    ("movimiento_detalles.movimiento_id -> movimientos.id", "core.movimiento_detalles", "movimiento_id", "core.movimientos", "id"),
    ("movimiento_detalles.producto_id -> productos.id", "core.movimiento_detalles", "producto_id", "core.productos", "id"),
    ("movimiento_detalles.unidad_id -> unidades.id", "core.movimiento_detalles", "unidad_id", "core.unidades", "id"),
    ("documentos.persona_id -> personas.id", "core.documentos", "persona_id", "core.personas", "id"),
    ("documentos.cliente_id -> personas.id", "core.documentos", "cliente_id", "core.personas", "id"),
    ("documentos.proveedor_id -> personas.id", "core.documentos", "proveedor_id", "core.personas", "id"),
    ("documentos.vendedor_id -> personas.id", "core.documentos", "vendedor_id", "core.personas", "id"),
    ("documentos.documento_relacionado_id -> documentos.id", "core.documentos", "documento_relacionado_id", "core.documentos", "id"),
    ("documento_detalles.documento_id -> documentos.id", "core.documento_detalles", "documento_id", "core.documentos", "id"),
    ("documento_detalles.producto_id -> productos.id", "core.documento_detalles", "producto_id", "core.productos", "id"),
    ("documento_detalles.cuenta_id -> cuentas_contables.id", "core.documento_detalles", "cuenta_id", "core.cuentas_contables", "id"),
    ("documento_detalles.centro_costo_id -> centros_costo.id", "core.documento_detalles", "centro_costo_id", "core.centros_costo", "id"),
    ("documento_cobros.documento_id -> documentos.id", "core.documento_cobros", "documento_id", "core.documentos", "id"),
    ("tickets_documentos.id -> documentos.id", "core.tickets_documentos", "id", "core.documentos", "id"),
    ("tickets_detalles.documento_id -> tickets_documentos.id", "core.tickets_detalles", "documento_id", "core.tickets_documentos", "id"),
    ("tickets_detalles.producto_id -> productos.id", "core.tickets_detalles", "producto_id", "core.productos", "id"),
    ("tickets_detalles.centro_costo_id -> centros_costo.id", "core.tickets_detalles", "centro_costo_id", "core.centros_costo", "id"),
    ("asiento_detalles.asiento_id -> asientos.id", "core.asiento_detalles", "asiento_id", "core.asientos", "id"),
    ("asiento_detalles.cuenta_id -> cuentas_contables.id", "core.asiento_detalles", "cuenta_id", "core.cuentas_contables", "id"),
    ("asiento_detalles.centro_costo_id -> centros_costo.id", "core.asiento_detalles", "centro_costo_id", "core.centros_costo", "id"),
]


def ensure_schema(conn) -> None:
    with conn.cursor() as cur:
        for statement in DDL_STATEMENTS:
            cur.execute(statement)
    conn.commit()


def truncate_backfill_tables(conn) -> None:
    statement = """
    TRUNCATE TABLE
        raw.resource_rows,
        core.tickets_items,
        core.tickets_detalles,
        core.tickets_documentos,
        core.documento_cobros,
        core.documento_detalles,
        core.documentos,
        core.movimiento_detalles,
        core.movimientos,
        core.asiento_detalles,
        core.asientos,
        core.productos,
        core.personas,
        core.periodos,
        core.centros_costo,
        core.unidades,
        core.marcas,
        core.bodegas,
        core.categorias,
        core.cuentas_contables,
        meta.watermarks
    CASCADE
    """
    with conn.cursor() as cur:
        cur.execute(statement)
    conn.commit()


def insert_rows(
    cur,
    table_name: str,
    columns: list[str],
    rows: list[dict[str, Any]],
    conflict_clause: str | None = None,
) -> int:
    if not rows:
        return 0
    statement = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
    if conflict_clause:
        statement = f"{statement} {conflict_clause}"
    values = [tuple(row.get(column) for column in columns) for row in rows]
    execute_values(cur, statement, values, page_size=500)
    return len(rows)


def build_raw_row(
    run_id: str,
    resource: str,
    entity_id: str,
    parent_entity_id: str | None,
    page_number: int,
    request_params: dict[str, Any],
    payload: Any,
    fetched_at: dt.datetime,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "resource": resource,
        "entity_id": entity_id,
        "parent_entity_id": parent_entity_id,
        "page_number": page_number,
        "request_params_jsonb": jsonb_value(request_params),
        "payload_jsonb": jsonb_value(payload),
        "fetched_at": fetched_at,
    }


def insert_extract_run(cur, row: dict[str, Any]) -> None:
    insert_rows(cur, "meta.extract_runs", META_EXTRACT_RUN_COLUMNS, [row])


def insert_load_metrics(cur, rows: list[dict[str, Any]]) -> None:
    insert_rows(cur, "meta.load_metrics", META_LOAD_METRIC_COLUMNS, rows)


def build_upsert_clause(table_name: str) -> str:
    primary_keys = CORE_PRIMARY_KEYS[table_name]
    non_key_columns = [column for column in CORE_TABLE_COLUMNS[table_name] if column not in primary_keys]
    assignments = ", ".join(f"{column} = EXCLUDED.{column}" for column in non_key_columns)
    return f"ON CONFLICT ({', '.join(primary_keys)}) DO UPDATE SET {assignments}"


def build_stub_row(table_name: str, entity_id: str, run_id: str, ingested_at: dt.datetime) -> dict[str, Any]:
    placeholder = f"__missing__:{entity_id}"
    row = {column: None for column in CORE_TABLE_COLUMNS[table_name]}
    row["run_id"] = run_id
    row["ingested_at"] = ingested_at
    if "id" in row:
        row["id"] = entity_id
    if table_name == "categorias":
        row["nombre"] = placeholder
    elif table_name == "bodegas":
        row["nombre"] = placeholder
        row["codigo"] = placeholder
    elif table_name == "marcas":
        row["nombre"] = placeholder
    elif table_name == "unidades":
        row["nombre"] = placeholder
    elif table_name == "cuentas_contables":
        row["nombre"] = placeholder
        row["codigo"] = placeholder
        row["tipo"] = "UNK"
    elif table_name == "centros_costo":
        row["nombre"] = placeholder
        row["codigo"] = placeholder
        row["estado"] = "UNK"
    elif table_name == "personas":
        row["razon_social"] = placeholder
        row["nombre_comercial"] = placeholder
    elif table_name == "productos":
        row["nombre"] = placeholder
        row["codigo"] = placeholder
        row["detalle_variantes_jsonb"] = jsonb_value([])
    elif table_name == "movimientos":
        row["codigo"] = placeholder
    elif table_name == "documentos":
        row["documento"] = placeholder
        row["tipo_documento"] = "UNK"
    elif table_name == "tickets_documentos":
        pass
    elif table_name == "asientos":
        row["glosa"] = placeholder
    return row


def ensure_reference_rows(cur, table_name: str, ids: set[str], run_id: str, ingested_at: dt.datetime) -> None:
    ids = {value for value in ids if value}
    if not ids:
        return
    with cur.connection.cursor() as lookup_cur:
        lookup_cur.execute(sql.SQL("SELECT id FROM core.{} WHERE id = ANY(%s)").format(sql.Identifier(table_name)), (list(ids),))
        existing = {row[0] for row in lookup_cur.fetchall()}
    missing = sorted(ids - existing)
    if not missing:
        return
    rows = [build_stub_row(table_name, entity_id, run_id, ingested_at) for entity_id in missing]
    insert_rows(
        cur,
        f"core.{table_name}",
        CORE_TABLE_COLUMNS[table_name],
        rows,
        conflict_clause=f"ON CONFLICT ({', '.join(CORE_PRIMARY_KEYS[table_name])}) DO NOTHING",
    )


def ensure_batch_references(cur, core_rows: dict[str, list[dict[str, Any]]], run_id: str, ingested_at: dt.datetime) -> None:
    ids_needed: dict[str, set[str]] = defaultdict(set)
    for table_name, dependencies in REFERENCE_COLUMNS.items():
        for row in core_rows.get(table_name, []):
            for target_table, column_name in dependencies:
                value = row.get(column_name)
                if value is not None:
                    ids_needed[target_table].add(str(value))
    for target_table in STUB_TARGET_ORDER:
        ensure_reference_rows(cur, target_table, ids_needed.get(target_table, set()), run_id, ingested_at)


def normalize_catalog_records(
    spec_key: str,
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_rows: list[dict[str, Any]] = []
    core_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    table_name = {
        "categoria": "categorias",
        "bodega": "bodegas",
        "marca": "marcas",
        "unidad": "unidades",
        "cuenta-contable": "cuentas_contables",
        "centro-costo": "centros_costo",
        "contabilidad/periodo": "periodos",
    }[spec_key]
    base_meta = resource_metadata(run_id, ingested_at)
    for record in records:
        entity_id = to_nonempty_text(record.get("id"))
        if not entity_id:
            continue
        raw_rows.append(build_raw_row(run_id, spec_key, entity_id, None, page_number, request_params, record, fetched_at))
        if table_name == "categorias":
            core_rows[table_name].append({
                "id": entity_id,
                "nombre": to_nonempty_text(record.get("nombre")),
                "padre_id": to_nonempty_text(record.get("padre_id")),
                "agrupar": to_bool(record.get("agrupar")),
                "tipo_producto": to_nonempty_text(record.get("tipo_producto")),
                "cuenta_venta": to_nonempty_text(record.get("cuenta_venta")),
                "cuenta_compra": to_nonempty_text(record.get("cuenta_compra")),
                "cuenta_inventario": to_nonempty_text(record.get("cuenta_inventario")),
                **base_meta,
            })
        elif table_name == "bodegas":
            core_rows[table_name].append({
                "id": entity_id,
                "codigo": to_nonempty_text(record.get("codigo")),
                "nombre": to_nonempty_text(record.get("nombre")),
                "venta": to_bool(record.get("venta")),
                "compra": to_bool(record.get("compra")),
                "produccion": to_bool(record.get("produccion")),
                **base_meta,
            })
        elif table_name == "marcas":
            core_rows[table_name].append({"id": entity_id, "nombre": to_nonempty_text(record.get("nombre")), **base_meta})
        elif table_name == "unidades":
            core_rows[table_name].append({"id": entity_id, "nombre": to_nonempty_text(record.get("nombre")), **base_meta})
        elif table_name == "cuentas_contables":
            core_rows[table_name].append({
                "id": entity_id,
                "nombre": to_nonempty_text(record.get("nombre")),
                "codigo": to_nonempty_text(record.get("codigo")),
                "tipo": to_nonempty_text(record.get("tipo")),
                **base_meta,
            })
        elif table_name == "centros_costo":
            core_rows[table_name].append({
                "id": entity_id,
                "nombre": to_nonempty_text(record.get("nombre")),
                "codigo": to_nonempty_text(record.get("codigo")),
                "tipo": to_nonempty_text(record.get("tipo")),
                "padre_id": to_nonempty_text(record.get("padre_id")),
                "estado": to_nonempty_text(record.get("estado")),
                **base_meta,
            })
        elif table_name == "periodos":
            core_rows[table_name].append({
                "id": entity_id,
                "fecha_inicio": parse_date(record.get("fecha_inicio")),
                "fecha_fin": parse_date(record.get("fecha_fin")),
                "estado": to_nonempty_text(record.get("estado")),
                "dia_cierre_mensual": to_int(record.get("dia_cierre_mensual")),
                **base_meta,
            })
    return raw_rows, core_rows


def normalize_persona_records(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_rows: list[dict[str, Any]] = []
    core_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    base_meta = resource_metadata(run_id, ingested_at)
    for record in records:
        entity_id = to_nonempty_text(record.get("id"))
        if not entity_id:
            continue
        raw_rows.append(build_raw_row(run_id, "persona", entity_id, None, page_number, request_params, record, fetched_at))
        vendedor = record.get("vendedor_asignado")
        vendedor_id = None
        vendedor_jsonb = None
        if isinstance(vendedor, dict):
            vendedor_id = to_nonempty_text(vendedor.get("id"))
            vendedor_jsonb = jsonb_value(vendedor)
        elif vendedor not in (None, ""):
            vendedor_id = to_nonempty_text(vendedor)
            vendedor_jsonb = jsonb_value(vendedor)
        core_rows["personas"].append({
            "id": entity_id,
            "tipo": to_nonempty_text(record.get("tipo")),
            "personaasociada_id": to_nonempty_text(record.get("personaasociada_id")),
            "es_cliente": to_bool(record.get("es_cliente")),
            "es_proveedor": to_bool(record.get("es_proveedor")),
            "es_extranjero": to_bool(record.get("es_extranjero")),
            "es_vendedor": to_bool(record.get("es_vendedor")),
            "es_empleado": to_bool(record.get("es_empleado")),
            "es_corporativo": to_bool(record.get("es_corporativo")),
            "ruc": to_nonempty_text(record.get("ruc")),
            "cedula": to_nonempty_text(record.get("cedula")),
            "placa": to_nonempty_text(record.get("placa")),
            "razon_social": to_nonempty_text(record.get("razon_social")),
            "nombre_comercial": to_nonempty_text(record.get("nombre_comercial")),
            "email": to_nonempty_text(record.get("email")),
            "telefonos": to_nonempty_text(record.get("telefonos")),
            "direccion": to_nonempty_text(record.get("direccion")),
            "pvp_default": to_nonempty_text(record.get("pvp_default")),
            "porcentaje_descuento": to_decimal(record.get("porcentaje_descuento")),
            "adicional1_cliente": to_nonempty_text(record.get("adicional1_cliente")),
            "adicional2_cliente": to_nonempty_text(record.get("adicional2_cliente")),
            "adicional3_cliente": to_nonempty_text(record.get("adicional3_cliente")),
            "adicional4_cliente": to_nonempty_text(record.get("adicional4_cliente")),
            "adicional1_proveedor": to_nonempty_text(record.get("adicional1_proveedor")),
            "adicional2_proveedor": to_nonempty_text(record.get("adicional2_proveedor")),
            "adicional3_proveedor": to_nonempty_text(record.get("adicional3_proveedor")),
            "adicional4_proveedor": to_nonempty_text(record.get("adicional4_proveedor")),
            "banco_codigo_id": to_nonempty_text(record.get("banco_codigo_id")),
            "tipo_cuenta": to_nonempty_text(record.get("tipo_cuenta")),
            "numero_tarjeta": to_nonempty_text(record.get("numero_tarjeta")),
            "aplicar_cupo": to_bool(record.get("aplicar_cupo")),
            "cuenta_por_cobrar_id": to_nonempty_text(record.get("cuenta_por_cobrar_id")),
            "cuenta_por_pagar_id": to_nonempty_text(record.get("cuenta_por_pagar_id")),
            "categoria_id": to_nonempty_text(record.get("categoria_id")),
            "categoria_nombre": to_nonempty_text(record.get("categoria_nombre")),
            "fecha_modificacion": parse_timestamp(record.get("fecha_modificacion")),
            "sueldo": to_decimal(record.get("sueldo")),
            "dias_credito": to_int(record.get("dias_credito")),
            "cupo_credito": to_decimal(record.get("cupo_credito")),
            "vendedor_asignado_id": vendedor_id,
            "vendedor_asignado_jsonb": vendedor_jsonb,
            **base_meta,
        })
    return raw_rows, core_rows


def normalize_producto_records(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_rows: list[dict[str, Any]] = []
    core_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    base_meta = resource_metadata(run_id, ingested_at)
    for record in records:
        entity_id = to_nonempty_text(record.get("id"))
        if not entity_id:
            continue
        raw_rows.append(build_raw_row(run_id, "producto", entity_id, None, page_number, request_params, record, fetched_at))
        core_rows["productos"].append({
            "id": entity_id,
            "unidad_id": to_nonempty_text(record.get("unidad")),
            "categoria_id": to_nonempty_text(record.get("categoria_id")),
            "codigo": to_nonempty_text(record.get("codigo")),
            "nombre": to_nonempty_text(record.get("nombre")),
            "codigo_auxiliar": to_nonempty_text(record.get("codigo_auxiliar")),
            "pvp_manual": to_bool(record.get("pvp_manual")),
            "pvp1": to_decimal(record.get("pvp1")),
            "pvp2": to_decimal(record.get("pvp2")),
            "pvp3": to_decimal(record.get("pvp3")),
            "pvp4": to_decimal(record.get("pvp4")),
            "generacion_automatica": to_bool(record.get("generacion_automatica")),
            "porcentaje_iva": to_decimal(record.get("porcentaje_iva")),
            "minimo": to_decimal(record.get("minimo")),
            "estado": to_nonempty_text(record.get("estado")),
            "tipo": to_nonempty_text(record.get("tipo")),
            "tipo_producto": to_nonempty_text(record.get("tipo_producto")),
            "para_pos": to_bool(record.get("para_pos")),
            "personalizado1": to_nonempty_text(record.get("personalizado1")),
            "personalizado2": to_nonempty_text(record.get("personalizado2")),
            "descripcion": to_nonempty_text(record.get("descripcion")),
            "codigo_barra": to_nonempty_text(record.get("codigo_barra")),
            "fecha_creacion": parse_timestamp(record.get("fecha_creacion")),
            "costo_maximo": to_decimal(record.get("costo_maximo")),
            "codigo_proveedor": to_nonempty_text(record.get("codigo_proveedor")),
            "lead_time": to_int(record.get("lead_time")),
            "cantidad_stock": to_decimal(record.get("cantidad_stock")),
            "cuenta_venta_id": to_nonempty_text(record.get("cuenta_venta_id")),
            "cuenta_compra_id": to_nonempty_text(record.get("cuenta_compra_id")),
            "cuenta_costo_id": to_nonempty_text(record.get("cuenta_costo_id")),
            "marca_id": to_nonempty_text(record.get("marca_id")),
            "marca_nombre": to_nonempty_text(record.get("marca_nombre")),
            "imagen": to_nonempty_text(record.get("imagen")),
            "producto_base_id": to_nonempty_text(record.get("producto_base_id")),
            "nombre_producto_base": to_nonempty_text(record.get("nombre_producto_base")),
            "detalle_variantes_jsonb": jsonb_value(record.get("detalle_variantes")),
            "codigo_sap": to_nonempty_text(record.get("codigo_sap")),
            "para_supereasy": to_bool(record.get("para_supereasy")),
            "pvp_supereasy": to_decimal(record.get("pvp_supereasy")),
            "para_comisariato": to_bool(record.get("para_comisariato")),
            "pvp_comisariato": to_decimal(record.get("pvp_comisariato")),
            "categoria_comisariato": to_nonempty_text(record.get("categoria_comisariato")),
            "id_integracion_proveedor": to_nonempty_text(record.get("id_integracion_proveedor")),
            "departamento": to_nonempty_text(record.get("departamento")),
            "descripcion_departamento": to_nonempty_text(record.get("descripcion_departamento")),
            "familia": to_nonempty_text(record.get("familia")),
            "descripcion_familia": to_nonempty_text(record.get("descripcion_familia")),
            "jerarquia": to_nonempty_text(record.get("jerarquia")),
            "descripcion_jerarquia": to_nonempty_text(record.get("descripcion_jerarquia")),
            "indicador_peso": to_bool(record.get("indicador_peso")),
            "pvp_peso": to_decimal(record.get("pvp_peso")),
            "peso_desde": to_decimal(record.get("peso_desde")),
            "peso_hasta": to_decimal(record.get("peso_hasta")),
            "porcentaje_ice": to_decimal(record.get("porcentaje_ice")),
            "valor_ice": to_decimal(record.get("valor_ice")),
            "campo_catalogo": to_nonempty_text(record.get("campo_catalogo")),
            "maneja_nombremanual": to_bool(record.get("maneja_nombremanual")),
            "porcentaje_servicio": None if record.get("porcentaje_servicio") in (None, "") else str(record.get("porcentaje_servicio")),
            **base_meta,
        })
    return raw_rows, core_rows


def normalize_movimiento_records(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_rows: list[dict[str, Any]] = []
    core_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    base_meta = resource_metadata(run_id, ingested_at)
    for record in records:
        movimiento_id = to_nonempty_text(record.get("id"))
        if not movimiento_id:
            continue
        raw_rows.append(build_raw_row(run_id, "movimiento-inventario", movimiento_id, None, page_number, request_params, record, fetched_at))
        core_rows["movimientos"].append({
            "id": movimiento_id,
            "codigo": to_nonempty_text(record.get("codigo")),
            "bodega_id": to_nonempty_text(record.get("bodega_id")),
            "tipo": to_nonempty_text(record.get("tipo")),
            "fecha": parse_date(record.get("fecha")),
            "generar_asiento": to_bool(record.get("generar_asiento")),
            "pos": to_nonempty_text(record.get("pos")),
            "cuenta_id": to_nonempty_text(record.get("cuenta_id")),
            "maneja_venta": to_bool(record.get("maneja_venta")),
            "descripcion": to_nonempty_text(record.get("descripcion")),
            "total": to_decimal(record.get("total")),
            "estado": to_nonempty_text(record.get("estado")),
            "bodega_destino_id": to_nonempty_text(record.get("bodega_destino_id")),
            "codigo_interno": to_nonempty_text(record.get("codigo_interno")),
            "proyecto": to_nonempty_text(record.get("proyecto")),
            **base_meta,
        })
        for detail_index, detail in enumerate(record.get("detalles") or []):
            raw_rows.append(build_raw_row(run_id, "movimiento-inventario.detalle", f"{movimiento_id}:{detail_index}", movimiento_id, page_number, request_params, detail, fetched_at))
            core_rows["movimiento_detalles"].append({
                "movimiento_id": movimiento_id,
                "detalle_index": detail_index,
                "serie": to_nonempty_text(detail.get("serie")),
                "producto_id": to_nonempty_text(detail.get("producto_id")),
                "edicion": to_nonempty_text(detail.get("edicion")),
                "precio": to_decimal(detail.get("precio")),
                "cantidad": to_decimal(detail.get("cantidad")),
                "unidad_id": to_nonempty_text(detail.get("unidad")),
                "costo_promedio": to_decimal(detail.get("costo_promedio")),
                **base_meta,
            })
    return raw_rows, core_rows


def derive_document_party_ids(record: dict[str, Any]) -> tuple[str | None, str | None, str | None, str | None]:
    persona_obj = record.get("persona")
    cliente_obj = record.get("cliente")
    vendedor_obj = record.get("vendedor")
    persona_id = to_nonempty_text(record.get("persona_id"))
    if not persona_id and isinstance(persona_obj, dict):
        persona_id = to_nonempty_text(persona_obj.get("id"))
    cliente_id = to_nonempty_text(cliente_obj.get("id")) if isinstance(cliente_obj, dict) else None
    if not cliente_id and to_nonempty_text(record.get("tipo_registro")) == "CLI":
        cliente_id = persona_id
    proveedor_id = persona_id if to_nonempty_text(record.get("tipo_registro")) == "PRO" else None
    vendedor_id = to_nonempty_text(record.get("vendedor_id"))
    if not vendedor_id and isinstance(vendedor_obj, dict):
        vendedor_id = to_nonempty_text(vendedor_obj.get("id"))
    return persona_id, cliente_id, proveedor_id, vendedor_id


def normalize_documento_records(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_rows: list[dict[str, Any]] = []
    core_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    base_meta = resource_metadata(run_id, ingested_at)
    for record in records:
        documento_id = to_nonempty_text(record.get("id"))
        if not documento_id:
            continue
        raw_rows.append(build_raw_row(run_id, "documento", documento_id, None, page_number, request_params, record, fetched_at))
        persona_id, cliente_id, proveedor_id, vendedor_id = derive_document_party_ids(record)
        core_rows["documentos"].append({
            "id": documento_id,
            "pos": to_nonempty_text(record.get("pos")),
            "persona_id": persona_id,
            "cliente_id": cliente_id,
            "proveedor_id": proveedor_id,
            "vendedor_id": vendedor_id,
            "fecha_emision": parse_date(record.get("fecha_emision")),
            "hora_emision": to_nonempty_text(record.get("hora_emision")),
            "tipo_registro": to_nonempty_text(record.get("tipo_registro")),
            "tipo_documento": to_nonempty_text(record.get("tipo_documento")),
            "documento": to_nonempty_text(record.get("documento")),
            "electronico": to_bool(record.get("electronico")),
            "autorizacion": to_nonempty_text(record.get("autorizacion")),
            "estado": to_nonempty_text(record.get("estado")),
            "subtotal_12": to_decimal(record.get("subtotal_12")),
            "subtotal_0": to_decimal(record.get("subtotal_0")),
            "iva": to_decimal(record.get("iva")),
            "ice": to_decimal(record.get("ice")),
            "servicio": to_decimal(record.get("servicio")),
            "total": to_decimal(record.get("total")),
            "reserva_relacionada": to_nonempty_text(record.get("reserva_relacionada")),
            "descripcion": to_nonempty_text(record.get("descripcion")),
            "referencia": to_nonempty_text(record.get("referencia")),
            "adicional1": to_nonempty_text(record.get("adicional1")),
            "adicional2": to_nonempty_text(record.get("adicional2")),
            "tarjeta_consumo_id": to_nonempty_text(record.get("tarjeta_consumo_id")),
            "logistica": to_nonempty_text(record.get("logistica")),
            "tipo_domicilio": to_nonempty_text(record.get("tipo_domicilio")),
            "orden_domicilio_id": to_nonempty_text(record.get("orden_domicilio_id")),
            "url_ride": to_nonempty_text(record.get("url_ride")),
            "tipo_descuento": to_nonempty_text(record.get("tipo_descuento")),
            "url_xml": to_nonempty_text(record.get("url_xml")),
            "placa": to_nonempty_text(record.get("placa")),
            "vendedor_identificacion": to_nonempty_text(record.get("vendedor_identificacion")),
            "fecha_creacion": parse_date(record.get("fecha_creacion")),
            "saldo_anticipo": to_decimal(record.get("saldo_anticipo")),
            "fecha_evento": parse_date(record.get("fecha_evento")),
            "hora_evento": to_nonempty_text(record.get("hora_evento")),
            "direccion_evento": to_nonempty_text(record.get("direccion_evento")),
            "pax": to_int(record.get("pax")),
            "fecha_vencimiento": parse_date(record.get("fecha_vencimiento")),
            "documento_relacionado_id": to_nonempty_text(record.get("documento_relacionado_id")),
            "firmado": to_bool(record.get("firmado")),
            "saldo": to_decimal(record.get("saldo")),
            "entregado": to_bool(record.get("entregado")),
            "anulado": to_bool(record.get("anulado")),
            "caja_id": to_nonempty_text(record.get("caja_id")),
            "fecha_modificacion": parse_date(record.get("fecha_modificacion")),
            "subtotal": to_decimal(record.get("subtotal")),
            "autorizado_sri": to_bool(record.get("autorizado_sri")),
            "enviado_sri": to_bool(record.get("enviado_sri")),
            "correo_enviado": to_bool(record.get("correo_enviado")),
            "retencion_autorizado_sri": to_bool(record.get("retencion_autorizado_sri")),
            "retencion_firmado": to_bool(record.get("retencion_firmado")),
            "retencion_enviado_sri": to_bool(record.get("retencion_enviado_sri")),
            "retencion_correo_enviado": to_bool(record.get("retencion_correo_enviado")),
            **base_meta,
        })
        for detail_index, detail in enumerate(record.get("detalles") or []):
            raw_rows.append(build_raw_row(run_id, "documento.detalle", f"{documento_id}:{detail_index}", documento_id, page_number, request_params, detail, fetched_at))
            core_rows["documento_detalles"].append({
                "documento_id": documento_id,
                "detalle_index": detail_index,
                "producto_id": to_nonempty_text(detail.get("producto_id")),
                "cuenta_id": to_nonempty_text(detail.get("cuenta_id")),
                "centro_costo_id": to_nonempty_text(detail.get("centro_costo_id")),
                "base_cero": to_decimal(detail.get("base_cero")),
                "base_no_gravable": to_decimal(detail.get("base_no_gravable")),
                "base_gravable": to_decimal(detail.get("base_gravable")),
                "cantidad": to_decimal(detail.get("cantidad")),
                "codigo_bien": to_nonempty_text(detail.get("codigo_bien")),
                "codigo_imp_iva": to_nonempty_text(detail.get("codigo_imp_iva")),
                "codigo_imp_ret": to_nonempty_text(detail.get("codigo_imp_ret")),
                "descripcion": to_nonempty_text(detail.get("descripcion")),
                "documento_ref": to_nonempty_text(detail.get("documento")),
                "formula_jsonb": jsonb_value(detail.get("formula")),
                "ibpnr": to_decimal(detail.get("ibpnr")),
                "nombre_manual": to_nonempty_text(detail.get("nombre_manual")),
                "peso": to_decimal(detail.get("peso")),
                "porcentaje_descuento": to_decimal(detail.get("porcentaje_descuento")),
                "porcentaje_ice": to_decimal(detail.get("porcentaje_ice")),
                "porcentaje_iva": to_decimal(detail.get("porcentaje_iva")),
                "precio": to_decimal(detail.get("precio")),
                "producto_descripcion": to_nonempty_text(detail.get("producto_descipcion") or detail.get("producto_descripcion")),
                "producto_nombre": to_nonempty_text(detail.get("producto_nombre")),
                "promocion_integracion_id": to_nonempty_text(detail.get("promocion_integracion_id")),
                "serie": to_nonempty_text(detail.get("serie")),
                "valor_ice": to_decimal(detail.get("valor_ice")),
                "volumen": to_decimal(detail.get("volumen")),
                **base_meta,
            })
        for cobro_index, cobro in enumerate(record.get("cobros") or []):
            raw_rows.append(build_raw_row(run_id, "documento.cobro", f"{documento_id}:{cobro_index}", documento_id, page_number, request_params, cobro, fetched_at))
            core_rows["documento_cobros"].append({
                "documento_id": documento_id,
                "cobro_index": cobro_index,
                "forma_cobro": to_nonempty_text(cobro.get("forma_cobro")),
                "numero_comprobante": to_nonempty_text(cobro.get("numero_comprobante")),
                "caja_id": to_nonempty_text(cobro.get("caja_id")),
                "monto": to_decimal(cobro.get("monto")),
                "numero_tarjeta": to_nonempty_text(cobro.get("numero_tarjeta")),
                "fecha": parse_date(cobro.get("fecha")),
                "fecha_creacion": parse_timestamp(cobro.get("fecha_creacion")),
                "nombre_tarjeta": to_nonempty_text(cobro.get("nombre_tarjeta")),
                "tipo_banco": to_nonempty_text(cobro.get("tipo_banco")),
                "bin_tarjeta": to_nonempty_text(cobro.get("bin_tarjeta")),
                "cuenta_bancaria_id": to_nonempty_text(cobro.get("cuenta_bancaria_id")),
                "monto_propina": to_decimal(cobro.get("monto_propina")),
                "numero_cheque": to_nonempty_text(cobro.get("numero_cheque")),
                "fecha_cheque": parse_date(cobro.get("fecha_cheque")),
                "tipo_ping": to_nonempty_text(cobro.get("tipo_ping")),
                "lote": to_nonempty_text(cobro.get("lote")),
                **base_meta,
            })
    return raw_rows, core_rows


def normalize_ticket_records(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_rows: list[dict[str, Any]] = []
    core_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    base_meta = resource_metadata(run_id, ingested_at)
    for record in records:
        documento_id = to_nonempty_text(record.get("id"))
        if not documento_id:
            continue
        raw_rows.append(build_raw_row(run_id, "documento/tickets", documento_id, None, page_number, request_params, record, fetched_at))
        core_rows["tickets_documentos"].append({"id": documento_id, "fecha_emision": parse_date(record.get("fecha_emision")), **base_meta})
        for detail_index, detail in enumerate(record.get("detalles") or []):
            raw_rows.append(build_raw_row(run_id, "documento/tickets.detalle", f"{documento_id}:{detail_index}", documento_id, page_number, request_params, detail, fetched_at))
            core_rows["tickets_detalles"].append({
                "documento_id": documento_id,
                "detalle_index": detail_index,
                "producto_id": to_nonempty_text(detail.get("producto_id")),
                "centro_costo_id": to_nonempty_text(detail.get("centro_costo_id")),
                "producto_nombre": to_nonempty_text(detail.get("producto_nombre")),
                "descripcion": to_nonempty_text(detail.get("descripcion")),
                "vendidos": to_int(detail.get("vendidos")),
                "leidos": to_int(detail.get("leidos")),
                **base_meta,
            })
            for ticket_index, ticket_item in enumerate(detail.get("tickets") or []):
                raw_rows.append(build_raw_row(run_id, "documento/tickets.item", f"{documento_id}:{detail_index}:{ticket_index}", f"{documento_id}:{detail_index}", page_number, request_params, ticket_item, fetched_at))
                core_rows["tickets_items"].append({
                    "documento_id": documento_id,
                    "detalle_index": detail_index,
                    "ticket_index": ticket_index,
                    "payload_jsonb": jsonb_value(ticket_item),
                    **base_meta,
                })
    return raw_rows, core_rows


def normalize_asiento_records(
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    raw_rows: list[dict[str, Any]] = []
    core_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    base_meta = resource_metadata(run_id, ingested_at)
    for record in records:
        asiento_id = to_nonempty_text(record.get("id"))
        if not asiento_id:
            continue
        raw_rows.append(build_raw_row(run_id, "contabilidad/asiento", asiento_id, None, page_number, request_params, record, fetched_at))
        core_rows["asientos"].append({"id": asiento_id, "glosa": to_nonempty_text(record.get("glosa")), "fecha": parse_date(record.get("fecha")), **base_meta})
        for detail_index, detail in enumerate(record.get("detalles") or []):
            raw_rows.append(build_raw_row(run_id, "contabilidad/asiento.detalle", f"{asiento_id}:{detail_index}", asiento_id, page_number, request_params, detail, fetched_at))
            core_rows["asiento_detalles"].append({
                "asiento_id": asiento_id,
                "detalle_index": detail_index,
                "cuenta_id": to_nonempty_text(detail.get("cuenta_id")),
                "centro_costo_id": to_nonempty_text(detail.get("centro_costo_id")),
                "tipo": to_nonempty_text(detail.get("tipo")),
                "valor": to_decimal(detail.get("valor")),
                **base_meta,
            })
    return raw_rows, core_rows


def normalize_records(
    spec: ResourceSpec,
    records: list[dict[str, Any]],
    run_id: str,
    ingested_at: dt.datetime,
    page_number: int,
    request_params: dict[str, Any],
    fetched_at: dt.datetime,
) -> tuple[list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
    if spec.key in CATALOG_RESOURCES:
        return normalize_catalog_records(spec.key, records, run_id, ingested_at, page_number, request_params, fetched_at)
    if spec.key == "persona":
        return normalize_persona_records(records, run_id, ingested_at, page_number, request_params, fetched_at)
    if spec.key == "producto":
        return normalize_producto_records(records, run_id, ingested_at, page_number, request_params, fetched_at)
    if spec.key == "movimiento-inventario":
        return normalize_movimiento_records(records, run_id, ingested_at, page_number, request_params, fetched_at)
    if spec.key == "documento":
        return normalize_documento_records(records, run_id, ingested_at, page_number, request_params, fetched_at)
    if spec.key == "documento/tickets":
        return normalize_ticket_records(records, run_id, ingested_at, page_number, request_params, fetched_at)
    if spec.key == "contabilidad/asiento":
        return normalize_asiento_records(records, run_id, ingested_at, page_number, request_params, fetched_at)
    raise RuntimeError(f"Unsupported resource spec: {spec.key}")


def fetch_resource_pages(client: ApiClient, spec: ResourceSpec) -> tuple[list[tuple[int, Any]], int]:
    if spec.response_kind == "flat":
        payload = client.get_json(spec.path)
        if not isinstance(payload, list):
            raise RuntimeError(f"Expected flat list for {spec.key}")
        return [(1, payload)], len(payload)
    pages = client.fetch_paginated_pages(spec.path)
    first_payload = pages[0][1] if pages else {}
    source_count = int(first_payload.get("count", 0) or 0) if isinstance(first_payload, dict) else 0
    return pages, source_count


def process_resource(conn, client: ApiClient, spec: ResourceSpec, run_id: str, ingested_at: dt.datetime, save_raw: bool) -> None:
    started_at = parse_iso_timestamp(iso_now())
    pages_fetched = 0
    source_count = 0
    raw_row_count = 0
    table_counts: dict[str, int] = defaultdict(int)
    try:
        pages, source_count = fetch_resource_pages(client, spec)
        pages_fetched = len(pages)
        with conn.cursor() as cur:
            cur.execute("SET CONSTRAINTS ALL DEFERRED")
            for page_number, payload in pages:
                fetched_at = parse_iso_timestamp(iso_now())
                request_params = {} if spec.response_kind == "flat" else {"page": page_number}
                page_records = payload if spec.response_kind == "flat" else payload.get("results", [])
                if not isinstance(page_records, list):
                    raise RuntimeError(f"Expected list of records for {spec.key}")
                raw_rows, core_rows = normalize_records(spec, page_records, run_id, ingested_at, page_number, request_params, fetched_at)
                ensure_batch_references(cur, core_rows, run_id, ingested_at)
                if save_raw:
                    raw_row_count += insert_rows(cur, "raw.resource_rows", RAW_RESOURCE_ROW_COLUMNS, raw_rows)
                for table_name, rows in core_rows.items():
                    table_counts[table_name] += insert_rows(
                        cur,
                        f"core.{table_name}",
                        CORE_TABLE_COLUMNS[table_name],
                        rows,
                        conflict_clause=build_upsert_clause(table_name),
                    )
            finished_at = parse_iso_timestamp(iso_now())
            metrics = [
                {"run_id": run_id, "resource": spec.key, "stage": "core", "table_name": name, "row_count": count, "measured_at": finished_at}
                for name, count in sorted(table_counts.items())
            ]
            if save_raw:
                metrics.append({"run_id": run_id, "resource": spec.key, "stage": "raw", "table_name": "resource_rows", "row_count": raw_row_count, "measured_at": finished_at})
            insert_load_metrics(cur, metrics)
            insert_extract_run(cur, {
                "run_id": run_id,
                "resource": spec.key,
                "mode": BACKFILL_MODE,
                "status": "success",
                "started_at": started_at,
                "finished_at": finished_at,
                "source_count": source_count,
                "pages_fetched": pages_fetched,
                "raw_row_count": raw_row_count,
                "table_counts_jsonb": jsonb_value(table_counts),
                "error_text": None,
                "created_at": finished_at,
            })
        conn.commit()
    except Exception as exc:
        conn.rollback()
        with conn.cursor() as cur:
            insert_extract_run(cur, {
                "run_id": run_id,
                "resource": spec.key,
                "mode": BACKFILL_MODE,
                "status": "failed",
                "started_at": started_at,
                "finished_at": parse_iso_timestamp(iso_now()),
                "source_count": source_count,
                "pages_fetched": pages_fetched,
                "raw_row_count": raw_row_count,
                "table_counts_jsonb": jsonb_value(table_counts),
                "error_text": str(exc),
                "created_at": parse_iso_timestamp(iso_now()),
            })
        conn.commit()
        raise


def refresh_watermarks(conn, run_id: str) -> None:
    resource_queries = {
        "cuenta-contable": "SELECT NULL::date, NULL::date",
        "categoria": "SELECT NULL::date, NULL::date",
        "bodega": "SELECT NULL::date, NULL::date",
        "marca": "SELECT NULL::date, NULL::date",
        "unidad": "SELECT NULL::date, NULL::date",
        "centro-costo": "SELECT NULL::date, NULL::date",
        "contabilidad/periodo": "SELECT MIN(fecha_inicio), MAX(fecha_fin) FROM core.periodos",
        "persona": "SELECT MIN(fecha_modificacion::date), MAX(fecha_modificacion::date) FROM core.personas",
        "producto": "SELECT MIN(fecha_creacion::date), MAX(fecha_creacion::date) FROM core.productos",
        "movimiento-inventario": "SELECT MIN(fecha), MAX(fecha) FROM core.movimientos",
        "documento": "SELECT MIN(fecha_emision), MAX(fecha_emision) FROM core.documentos",
        "documento/tickets": "SELECT MIN(fecha_emision), MAX(fecha_emision) FROM core.tickets_documentos",
        "contabilidad/asiento": "SELECT MIN(fecha), MAX(fecha) FROM core.asientos",
    }
    with conn.cursor() as cur:
        rows: list[dict[str, Any]] = []
        updated_at = parse_iso_timestamp(iso_now())
        for resource, query in resource_queries.items():
            cur.execute(query)
            min_date, max_date = cur.fetchone()
            rows.append({
                "resource": resource,
                "last_run_id": run_id,
                "min_record_date": min_date,
                "max_record_date": max_date,
                "updated_at": updated_at,
            })
        insert_rows(
            cur,
            "meta.watermarks",
            META_WATERMARK_COLUMNS,
            rows,
            conflict_clause="""
            ON CONFLICT (resource) DO UPDATE SET
                last_run_id = EXCLUDED.last_run_id,
                min_record_date = EXCLUDED.min_record_date,
                max_record_date = EXCLUDED.max_record_date,
                updated_at = EXCLUDED.updated_at
            """,
        )
    conn.commit()


def create_reporting_views(conn) -> None:
    fk_view_parts: list[str] = []
    for relation_name, src_table, src_column, dst_table, dst_column in FK_HEALTH_CHECKS:
        escaped_name = relation_name.replace("'", "''")
        fk_view_parts.append(
            f"""
            SELECT '{escaped_name}' AS relation_name, COUNT(*)::bigint AS orphan_count
            FROM {src_table} src
            LEFT JOIN {dst_table} dst ON dst.{dst_column} = src.{src_column}
            WHERE src.{src_column} IS NOT NULL AND dst.{dst_column} IS NULL
            """
        )
    fk_view_sql = "\nUNION ALL\n".join(fk_view_parts)
    statements = [
        """
        CREATE OR REPLACE VIEW reporting.v_load_summary AS
        SELECT
            er.run_id,
            er.resource,
            er.mode,
            er.status,
            lm.stage,
            lm.table_name,
            lm.row_count,
            lm.measured_at
        FROM meta.extract_runs er
        LEFT JOIN meta.load_metrics lm
          ON lm.run_id = er.run_id AND lm.resource = er.resource
        """,
        """
        CREATE OR REPLACE VIEW reporting.v_temporal_coverage AS
        SELECT 'personas' AS resource, COUNT(*)::bigint AS row_count, MIN(fecha_modificacion::date) AS min_date, MAX(fecha_modificacion::date) AS max_date FROM core.personas
        UNION ALL
        SELECT 'productos', COUNT(*)::bigint, MIN(fecha_creacion::date), MAX(fecha_creacion::date) FROM core.productos
        UNION ALL
        SELECT 'movimientos', COUNT(*)::bigint, MIN(fecha), MAX(fecha) FROM core.movimientos
        UNION ALL
        SELECT 'documentos', COUNT(*)::bigint, MIN(fecha_emision), MAX(fecha_emision) FROM core.documentos
        UNION ALL
        SELECT 'tickets_documentos', COUNT(*)::bigint, MIN(fecha_emision), MAX(fecha_emision) FROM core.tickets_documentos
        UNION ALL
        SELECT 'asientos', COUNT(*)::bigint, MIN(fecha), MAX(fecha) FROM core.asientos
        UNION ALL
        SELECT 'periodos', COUNT(*)::bigint, MIN(fecha_inicio), MAX(fecha_fin) FROM core.periodos
        """,
        f"CREATE OR REPLACE VIEW reporting.v_fk_health AS {fk_view_sql}",
        """
        CREATE OR REPLACE VIEW reporting.v_personas_resumen AS
        SELECT 'cliente' AS rol, COUNT(*)::bigint AS total FROM core.personas WHERE es_cliente IS TRUE
        UNION ALL
        SELECT 'proveedor', COUNT(*)::bigint FROM core.personas WHERE es_proveedor IS TRUE
        UNION ALL
        SELECT 'vendedor', COUNT(*)::bigint FROM core.personas WHERE es_vendedor IS TRUE
        UNION ALL
        SELECT 'empleado', COUNT(*)::bigint FROM core.personas WHERE es_empleado IS TRUE
        """,
        """
        CREATE OR REPLACE VIEW reporting.v_productos_resumen AS
        SELECT
            COALESCE(p.estado, '(sin estado)') AS estado,
            COALESCE(c.nombre, '(sin categoria)') AS categoria,
            COUNT(*)::bigint AS total_productos,
            COALESCE(SUM(p.cantidad_stock), 0)::numeric(18,6) AS stock_total
        FROM core.productos p
        LEFT JOIN core.categorias c ON c.id = p.categoria_id
        GROUP BY 1, 2
        ORDER BY total_productos DESC, estado, categoria
        """,
        """
        CREATE OR REPLACE VIEW reporting.v_movimientos_resumen AS
        SELECT
            COALESCE(m.tipo, '(sin tipo)') AS tipo,
            COALESCE(b.nombre, '(sin bodega)') AS bodega,
            COUNT(*)::bigint AS total_movimientos,
            COALESCE(SUM(m.total), 0)::numeric(18,6) AS valor_total
        FROM core.movimientos m
        LEFT JOIN core.bodegas b ON b.id = m.bodega_id
        GROUP BY 1, 2
        ORDER BY total_movimientos DESC, tipo, bodega
        """,
        """
        CREATE OR REPLACE VIEW reporting.v_documentos_resumen AS
        SELECT
            COALESCE(tipo_documento, '(sin tipo)') AS tipo_documento,
            COALESCE(estado, '(sin estado)') AS estado,
            COUNT(*)::bigint AS total_documentos,
            COALESCE(SUM(total), 0)::numeric(18,6) AS monto_total
        FROM core.documentos
        GROUP BY 1, 2
        ORDER BY total_documentos DESC, tipo_documento, estado
        """,
        """
        CREATE OR REPLACE VIEW reporting.v_asientos_resumen AS
        SELECT fecha, COUNT(*)::bigint AS total_asientos
        FROM core.asientos
        GROUP BY fecha
        ORDER BY fecha
        """,
    ]
    with conn.cursor() as cur:
        for statement in statements:
            cur.execute(statement)
    conn.commit()


def query_rows(conn, statement: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(statement, params)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def markdown_table(rows: list[dict[str, Any]], columns: list[str]) -> str:
    if not rows:
        return "_Sin datos_"
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    body: list[str] = []
    for row in rows:
        body.append("| " + " | ".join("" if row.get(column) is None else str(row.get(column)) for column in columns) + " |")
    return "\n".join([header, separator, *body])


def generate_final_report(conn, report_path: Path, run_id: str, args: argparse.Namespace, config: PgConfig) -> None:
    primary_table_by_resource = {
        "cuenta-contable": "cuentas_contables",
        "categoria": "categorias",
        "bodega": "bodegas",
        "marca": "marcas",
        "unidad": "unidades",
        "centro-costo": "centros_costo",
        "contabilidad/periodo": "periodos",
        "persona": "personas",
        "producto": "productos",
        "movimiento-inventario": "movimientos",
        "documento": "documentos",
        "documento/tickets": "tickets_documentos",
        "contabilidad/asiento": "asientos",
    }
    run_rows = query_rows(
        conn,
        """
        SELECT resource, status, source_count, pages_fetched, raw_row_count, started_at, finished_at
        FROM meta.extract_runs
        WHERE run_id = %s
        ORDER BY started_at, resource
        """,
        (run_id,),
    )
    load_rows = query_rows(
        conn,
        """
        SELECT resource, stage, table_name, row_count
        FROM reporting.v_load_summary
        WHERE run_id = %s
        ORDER BY stage, table_name
        """,
        (run_id,),
    )
    temporal_rows = query_rows(conn, "SELECT resource, row_count, min_date, max_date FROM reporting.v_temporal_coverage ORDER BY resource")
    fk_rows = query_rows(conn, "SELECT relation_name, orphan_count FROM reporting.v_fk_health ORDER BY relation_name")
    personas_rows = query_rows(conn, "SELECT rol, total FROM reporting.v_personas_resumen ORDER BY rol")
    productos_rows = query_rows(conn, "SELECT estado, categoria, total_productos, stock_total FROM reporting.v_productos_resumen LIMIT 15")
    movimientos_rows = query_rows(conn, "SELECT tipo, bodega, total_movimientos, valor_total FROM reporting.v_movimientos_resumen LIMIT 15")
    documentos_rows = query_rows(conn, "SELECT tipo_documento, estado, total_documentos, monto_total FROM reporting.v_documentos_resumen LIMIT 15")
    asientos_rows = query_rows(conn, "SELECT fecha, total_asientos FROM reporting.v_asientos_resumen ORDER BY fecha DESC LIMIT 15")
    source_compare_rows = []
    for row in run_rows:
        table_name = primary_table_by_resource[row["resource"]]
        core_count = query_rows(conn, f"SELECT COUNT(*)::bigint AS total FROM core.{table_name}")[0]["total"]
        source_compare_rows.append({
            "resource": row["resource"],
            "source_count": row["source_count"],
            "core_primary_count": core_count,
            "difference": None if row["source_count"] is None else int(row["source_count"]) - int(core_count),
        })
    placeholder_rows = query_rows(
        conn,
        """
        SELECT 'categorias' AS table_name, COUNT(*)::bigint AS placeholder_count FROM core.categorias WHERE nombre LIKE '__missing__:%'
        UNION ALL
        SELECT 'cuentas_contables', COUNT(*)::bigint FROM core.cuentas_contables WHERE nombre LIKE '__missing__:%'
        UNION ALL
        SELECT 'personas', COUNT(*)::bigint FROM core.personas WHERE razon_social LIKE '__missing__:%'
        UNION ALL
        SELECT 'productos', COUNT(*)::bigint FROM core.productos WHERE nombre LIKE '__missing__:%'
        UNION ALL
        SELECT 'documentos', COUNT(*)::bigint FROM core.documentos WHERE documento LIKE '__missing__:%'
        UNION ALL
        SELECT 'bodegas', COUNT(*)::bigint FROM core.bodegas WHERE nombre LIKE '__missing__:%'
        UNION ALL
        SELECT 'unidades', COUNT(*)::bigint FROM core.unidades WHERE nombre LIKE '__missing__:%'
        UNION ALL
        SELECT 'marcas', COUNT(*)::bigint FROM core.marcas WHERE nombre LIKE '__missing__:%'
        UNION ALL
        SELECT 'centros_costo', COUNT(*)::bigint FROM core.centros_costo WHERE nombre LIKE '__missing__:%'
        """
    )
    critical_rows = query_rows(
        conn,
        """
        SELECT 'documentos_sin_persona_id' AS indicador, COUNT(*)::bigint AS total FROM core.documentos WHERE persona_id IS NULL
        UNION ALL
        SELECT 'documento_detalles_producto_id_null_permitido', COUNT(*)::bigint FROM core.documento_detalles WHERE producto_id IS NULL
        UNION ALL
        SELECT 'tickets_detalles_producto_id_null_permitido', COUNT(*)::bigint FROM core.tickets_detalles WHERE producto_id IS NULL
        UNION ALL
        SELECT 'movimiento_detalles_producto_id_null', COUNT(*)::bigint FROM core.movimiento_detalles WHERE producto_id IS NULL
        UNION ALL
        SELECT 'tickets_items', COUNT(*)::bigint FROM core.tickets_items
        """
    )
    report_lines = [
        "# Informe Final",
        "",
        "## Infraestructura",
        "",
        f"- Fecha de corrida: {dt.date.today().isoformat()}",
        f"- Base PostgreSQL: `{config.db_name}` en `{config.host}:{config.port}`",
        f"- Modo: `{args.mode}`",
        f"- API base: `{args.base_url}`",
        f"- Raw auditado: `{'si' if args.save_raw else 'no'}`",
        f"- Run ID: `{run_id}`",
        "",
        "## Recursos procesados",
        "",
        markdown_table(run_rows, ["resource", "status", "source_count", "pages_fetched", "raw_row_count", "started_at", "finished_at"]),
        "",
        "## Filas cargadas por tabla",
        "",
        markdown_table(load_rows, ["resource", "stage", "table_name", "row_count"]),
        "",
        "## Fuente vs tabla principal",
        "",
        markdown_table(source_compare_rows, ["resource", "source_count", "core_primary_count", "difference"]),
        "",
        "## Cobertura temporal",
        "",
        markdown_table(temporal_rows, ["resource", "row_count", "min_date", "max_date"]),
        "",
        "## Salud relacional",
        "",
        markdown_table(fk_rows, ["relation_name", "orphan_count"]),
        "",
        "## Nulos y observaciones críticas",
        "",
        markdown_table(critical_rows, ["indicador", "total"]),
        "",
        "## Placeholders relacionales",
        "",
        markdown_table(placeholder_rows, ["table_name", "placeholder_count"]),
        "",
        "## Resumen de negocio",
        "",
        "### Personas por rol",
        "",
        markdown_table(personas_rows, ["rol", "total"]),
        "",
        "### Productos por estado y categoría",
        "",
        markdown_table(productos_rows, ["estado", "categoria", "total_productos", "stock_total"]),
        "",
        "### Movimientos por tipo y bodega",
        "",
        markdown_table(movimientos_rows, ["tipo", "bodega", "total_movimientos", "valor_total"]),
        "",
        "### Documentos por tipo y estado",
        "",
        markdown_table(documentos_rows, ["tipo_documento", "estado", "total_documentos", "monto_total"]),
        "",
        "### Asientos por fecha",
        "",
        markdown_table(asientos_rows, ["fecha", "total_asientos"]),
        "",
        "## Incidencias",
        "",
        "- El backfill histórico usa paginación exhaustiva completa y no depende de filtros de fecha del backend.",
        "- El endpoint `movimiento-inventario` reportó un `count` mayor al número final de IDs únicos materializados; se conservó la versión única de cada movimiento.",
        "- `documento_detalles.producto_id` y `tickets_detalles.producto_id` aceptan `null`; esos registros se conservaron sin romper integridad.",
        "- Los campos sin catálogo validado quedaron como atributos simples: `caja_id`, `cuenta_bancaria_id`, `banco_codigo_id`, `tarjeta_consumo_id`, `logistica`, `orden_domicilio_id`, `proyecto`.",
        "- Cuando una referencia no vino en el catálogo origen, se creó un placeholder controlado para mantener la FK y dejar trazabilidad de la anomalía.",
        "- `tickets_items` se cargó únicamente cuando el payload incluyó elementos en `tickets[]`; en ausencia de items, el detalle igualmente quedó preservado.",
        "",
    ]
    report_path.write_text("\n".join(report_lines), encoding="utf-8")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Contifico historical backfill to PostgreSQL")
    parser.add_argument("--mode", choices=(BACKFILL_MODE,), required=True)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--report-out", default="final_report.md")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--save-raw", action="store_true")
    parser.add_argument("--max-workers", type=int, default=DEFAULT_MAX_WORKERS)
    return parser


def run_backfill(args: argparse.Namespace) -> int:
    authorization = os.getenv("CONTIFICO_AUTHORIZATION")
    if not authorization:
        raise RuntimeError("Missing CONTIFICO_AUTHORIZATION environment variable")
    config = pg_config_from_env(args.db_name)
    ensure_database_exists(config)
    client = ApiClient(args.base_url, authorization, max_workers=args.max_workers)
    print_progress("Validating API access...")
    validate_status(client)
    with open_connection(config, config.db_name) as conn:
        print_progress("Ensuring PostgreSQL schema...")
        ensure_schema(conn)
        print_progress("Truncating previous backfill data...")
        truncate_backfill_tables(conn)
        run_id = dt.datetime.now(UTC).strftime("%Y%m%d%H%M%S")
        ingested_at = parse_iso_timestamp(iso_now())
        for resource_key in RESOURCE_ORDER:
            spec = RESOURCE_SPECS_BY_KEY[resource_key]
            print_progress(f"Loading {resource_key} into PostgreSQL...")
            process_resource(conn, client, spec, run_id, ingested_at, args.save_raw)
        print_progress("Refreshing watermarks and reporting views...")
        refresh_watermarks(conn, run_id)
        create_reporting_views(conn)
        report_path = Path(args.report_out).resolve()
        generate_final_report(conn, report_path, run_id, args, config)
        print_progress(f"Final report generated at {report_path}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_backfill(args)


if __name__ == "__main__":
    sys.exit(main())
