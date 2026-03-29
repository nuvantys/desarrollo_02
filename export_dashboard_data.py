from __future__ import annotations

import argparse
import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

from psycopg2 import sql

from contifico_pg_backfill import DEFAULT_DB_NAME, open_connection, pg_config_from_env


def json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def query_rows(conn, statement: str, params: tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(statement, params)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]


def query_value(conn, statement: str, params: tuple[Any, ...] | None = None, default: Any = None) -> Any:
    rows = query_rows(conn, statement, params)
    if not rows:
        return default
    first = rows[0]
    if not first:
        return default
    return next(iter(first.values()))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")


def payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, default=json_default).encode("utf-8"))


def payload_top_level_rows(payload: dict[str, Any]) -> int:
    return sum(len(value) for value in payload.values() if isinstance(value, list))


def base_metadata(conn) -> dict[str, Any]:
    manifest_row = query_rows(
        conn,
        """
        SELECT
            max(last_run_id) AS run_id,
            min(min_record_date) AS coverage_min,
            max(max_record_date) AS coverage_max,
            max(updated_at) AS generated_at
        FROM meta.watermarks
        """,
    )[0]
    return manifest_row


def filters_available(conn) -> dict[str, list[dict[str, Any]]]:
    return {
        "document_types": query_rows(
            conn,
            """
            SELECT tipo_documento AS value, tipo_documento AS label, COUNT(*)::bigint AS count
            FROM core.documentos
            GROUP BY tipo_documento
            ORDER BY count DESC, value
            """,
        ),
        "document_states": query_rows(
            conn,
            """
            SELECT estado AS value, estado AS label, COUNT(*)::bigint AS count
            FROM core.documentos
            GROUP BY estado
            ORDER BY count DESC, value
            """,
        ),
        "bodegas": query_rows(
            conn,
            """
            SELECT b.id AS value, b.nombre AS label, COUNT(m.id)::bigint AS count
            FROM core.bodegas b
            LEFT JOIN core.movimientos m ON m.bodega_id = b.id
            GROUP BY b.id, b.nombre
            ORDER BY count DESC, label
            """,
        ),
        "categories": query_rows(
            conn,
            """
            SELECT c.id AS value, c.nombre AS label, COUNT(p.id)::bigint AS count
            FROM core.categorias c
            LEFT JOIN core.productos p ON p.categoria_id = c.id
            GROUP BY c.id, c.nombre
            ORDER BY count DESC, label
            """,
        ),
        "accounts": query_rows(
            conn,
            """
            SELECT a.id AS value, a.nombre AS label, COUNT(d.asiento_id)::bigint AS count
            FROM core.cuentas_contables a
            LEFT JOIN core.asiento_detalles d ON d.cuenta_id = a.id
            GROUP BY a.id, a.nombre
            ORDER BY count DESC, label
            LIMIT 50
            """,
        ),
        "cost_centers": query_rows(
            conn,
            """
            SELECT c.id AS value, c.nombre AS label, COUNT(d.asiento_id)::bigint AS count
            FROM core.centros_costo c
            LEFT JOIN core.asiento_detalles d ON d.centro_costo_id = c.id
            GROUP BY c.id, c.nombre
            ORDER BY count DESC, label
            """,
        ),
    }


def build_manifest(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    counts = query_rows(
        conn,
        """
        SELECT 'documentos' AS key, COUNT(*)::bigint AS value FROM core.documentos
        UNION ALL SELECT 'personas', COUNT(*)::bigint FROM core.personas
        UNION ALL SELECT 'productos', COUNT(*)::bigint FROM core.productos
        UNION ALL SELECT 'movimientos', COUNT(*)::bigint FROM core.movimientos
        UNION ALL SELECT 'asientos', COUNT(*)::bigint FROM core.asientos
        UNION ALL SELECT 'documento_detalles', COUNT(*)::bigint FROM core.documento_detalles
        UNION ALL SELECT 'asiento_detalles', COUNT(*)::bigint FROM core.asiento_detalles
        """
    )
    alerts = [
        {
            "level": "warning",
            "title": "Anomalía en movimientos",
            "message": "El endpoint de movimiento-inventario reporta más filas origen que IDs únicos materializados.",
            "metric": query_rows(
                conn,
                """
                SELECT (
                    (SELECT max(source_count) FROM meta.extract_runs WHERE resource = 'movimiento-inventario')
                    - (SELECT COUNT(*) FROM core.movimientos)
                )::bigint AS value
                """,
            )[0]["value"],
        },
        {
            "level": "info",
            "title": "Salud relacional",
            "message": "Todas las relaciones validadas quedaron sin huérfanos en el modelo PostgreSQL.",
            "metric": query_rows(conn, "SELECT SUM(orphan_count)::bigint AS value FROM reporting.v_fk_health")[0]["value"],
        },
        {
            "level": "info",
            "title": "Placeholder detectado",
            "message": "Se generó un placeholder controlado en categorías para preservar integridad referencial.",
            "metric": query_rows(conn, "SELECT COUNT(*)::bigint AS value FROM core.categorias WHERE nombre LIKE '__missing__:%'")[0]["value"],
        },
    ]
    return {
        **meta,
        "version": "1.0.0",
        "counts": counts,
        "alerts": alerts,
        "filters_available": filters,
        "data_files": [
            "overview.json",
            "commercial.json",
            "customers.json",
            "products.json",
            "inventory.json",
            "accounting.json",
            "quality.json",
            "technical.json",
            "database.json",
            "tables.json",
        ],
    }


def build_overview(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    hero_metrics = query_rows(
        conn,
        """
        SELECT 'documentos' AS key, COUNT(*)::bigint AS value, 'Documentos históricos' AS label FROM core.documentos
        UNION ALL SELECT 'ventas_total', COALESCE(SUM(total),0)::numeric(18,2), 'Monto acumulado documentos' FROM core.documentos
        UNION ALL SELECT 'clientes_activos', COUNT(DISTINCT cliente_id)::bigint, 'Clientes con actividad' FROM core.documentos WHERE cliente_id IS NOT NULL
        UNION ALL SELECT 'productos', COUNT(*)::bigint, 'Productos' FROM core.productos
        UNION ALL SELECT 'movimientos_unicos', COUNT(*)::bigint, 'Movimientos únicos' FROM core.movimientos
        UNION ALL SELECT 'asientos', COUNT(*)::bigint, 'Asientos contables' FROM core.asientos
        """
    )
    yearly_story = query_rows(
        conn,
        """
        SELECT
            date_trunc('year', fecha_emision)::date AS period,
            COUNT(*)::bigint AS documentos,
            COALESCE(SUM(total),0)::numeric(18,2) AS monto_total
        FROM core.documentos
        GROUP BY 1
        ORDER BY 1
        """
    )
    monthly_story = query_rows(
        conn,
        """
        SELECT
            date_trunc('month', d.fecha_emision)::date AS period,
            COUNT(*)::bigint AS documentos,
            COALESCE(SUM(d.total),0)::numeric(18,2) AS monto_total,
            COALESCE(a.total_asientos, 0)::bigint AS asientos
        FROM core.documentos d
        LEFT JOIN (
            SELECT date_trunc('month', fecha)::date AS period, COUNT(*)::bigint AS total_asientos
            FROM core.asientos
            GROUP BY 1
        ) a ON a.period = date_trunc('month', d.fecha_emision)::date
        GROUP BY 1, a.total_asientos
        ORDER BY 1
        """
    )
    narrative = [
        "La cobertura histórica consolidada arranca en 2019 y llega hasta 2026.",
        "El peso operativo principal está en documentos y asientos, que concentran la mayor densidad del histórico.",
        "Inventario presenta una anomalía de conteo en origen, ya normalizada a IDs únicos dentro del modelo analítico.",
    ]
    return {
        **meta,
        "filters_available": filters,
        "hero_metrics": hero_metrics,
        "yearly_story": yearly_story,
        "monthly_story": monthly_story,
        "narrative": narrative,
    }


def build_commercial(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        **meta,
        "filters_available": {
            "document_types": filters["document_types"],
            "document_states": filters["document_states"],
        },
        "document_facts": query_rows(
            conn,
            """
            SELECT
                d.id,
                d.fecha_emision::date AS date,
                d.tipo_documento,
                d.estado,
                COALESCE(d.total,0)::numeric(18,2) AS total,
                d.cliente_id,
                COALESCE(c.razon_social, 'Sin cliente') AS cliente_nombre,
                d.vendedor_id,
                COALESCE(v.razon_social, 'Sin vendedor') AS vendedor_nombre
            FROM core.documentos d
            LEFT JOIN core.personas c ON c.id = d.cliente_id
            LEFT JOIN core.personas v ON v.id = d.vendedor_id
            ORDER BY d.fecha_emision
            """
        ),
        "document_mix": query_rows(
            conn,
            """
            SELECT tipo_documento, estado, COUNT(*)::bigint AS documentos, COALESCE(SUM(total),0)::numeric(18,2) AS monto_total
            FROM core.documentos
            GROUP BY tipo_documento, estado
            ORDER BY documentos DESC, tipo_documento, estado
            """
        ),
        "monthly_revenue": query_rows(
            conn,
            """
            SELECT date_trunc('month', fecha_emision)::date AS period, COUNT(*)::bigint AS documentos, COALESCE(SUM(total),0)::numeric(18,2) AS monto_total
            FROM core.documentos
            GROUP BY 1
            ORDER BY 1
            """
        ),
        "seller_performance": query_rows(
            conn,
            """
            SELECT
                COALESCE(v.razon_social, 'Sin vendedor') AS vendedor,
                COUNT(*)::bigint AS documentos,
                COALESCE(SUM(d.total),0)::numeric(18,2) AS monto_total
            FROM core.documentos d
            LEFT JOIN core.personas v ON v.id = d.vendedor_id
            GROUP BY 1
            ORDER BY monto_total DESC
            LIMIT 20
            """
        ),
    }


def build_customers(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        **meta,
        "filters_available": {
            "document_types": filters["document_types"],
            "document_states": filters["document_states"],
        },
        "role_mix": query_rows(conn, "SELECT rol, total FROM reporting.v_personas_resumen ORDER BY rol"),
        "customer_stats": query_rows(
            conn,
            """
            SELECT
                d.cliente_id,
                COALESCE(p.razon_social, 'Sin cliente') AS cliente_nombre,
                COUNT(*)::bigint AS documentos,
                COALESCE(SUM(d.total),0)::numeric(18,2) AS monto_total,
                MIN(d.fecha_emision)::date AS primera_fecha,
                MAX(d.fecha_emision)::date AS ultima_fecha,
                COUNT(DISTINCT date_trunc('month', d.fecha_emision))::bigint AS meses_activos
            FROM core.documentos d
            LEFT JOIN core.personas p ON p.id = d.cliente_id
            WHERE d.cliente_id IS NOT NULL
            GROUP BY d.cliente_id, p.razon_social
            ORDER BY monto_total DESC
            LIMIT 200
            """
        ),
        "customer_concentration": query_rows(
            conn,
            """
            WITH ranked AS (
                SELECT
                    COALESCE(p.razon_social, 'Sin cliente') AS cliente_nombre,
                    COALESCE(SUM(d.total),0)::numeric(18,2) AS monto_total,
                    ROW_NUMBER() OVER (ORDER BY COALESCE(SUM(d.total),0) DESC) AS rn
                FROM core.documentos d
                LEFT JOIN core.personas p ON p.id = d.cliente_id
                WHERE d.cliente_id IS NOT NULL
                GROUP BY p.razon_social
            )
            SELECT cliente_nombre, monto_total, rn
            FROM ranked
            WHERE rn <= 25
            ORDER BY rn
            """
        ),
        "monthly_active_customers": query_rows(
            conn,
            """
            SELECT
                date_trunc('month', fecha_emision)::date AS period,
                COUNT(DISTINCT cliente_id)::bigint AS clientes_activos
            FROM core.documentos
            WHERE cliente_id IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            """
        ),
    }


def build_products(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        **meta,
        "filters_available": {
            "document_types": filters["document_types"],
            "document_states": filters["document_states"],
            "categories": filters["categories"],
        },
        "line_facts": query_rows(
            conn,
            """
            SELECT
                dd.documento_id,
                d.fecha_emision::date AS date,
                d.tipo_documento,
                d.estado,
                dd.producto_id,
                COALESCE(dd.producto_nombre, p.nombre, 'Sin producto') AS producto_nombre,
                COALESCE(c.nombre, 'Sin categoría') AS categoria_nombre,
                COALESCE(m.nombre, 'Sin marca') AS marca_nombre,
                COALESCE(dd.cantidad,0)::numeric(18,2) AS cantidad,
                COALESCE(dd.precio,0)::numeric(18,2) AS precio,
                (COALESCE(dd.cantidad,0) * COALESCE(dd.precio,0))::numeric(18,2) AS importe
            FROM core.documento_detalles dd
            INNER JOIN core.documentos d ON d.id = dd.documento_id
            LEFT JOIN core.productos p ON p.id = dd.producto_id
            LEFT JOIN core.categorias c ON c.id = p.categoria_id
            LEFT JOIN core.marcas m ON m.id = p.marca_id
            ORDER BY d.fecha_emision
            """
        ),
        "category_stock": query_rows(
            conn,
            """
            SELECT
                COALESCE(c.nombre, 'Sin categoría') AS categoria_nombre,
                COUNT(*)::bigint AS productos,
                COALESCE(SUM(p.cantidad_stock),0)::numeric(18,2) AS stock_total
            FROM core.productos p
            LEFT JOIN core.categorias c ON c.id = p.categoria_id
            GROUP BY 1
            ORDER BY stock_total DESC, productos DESC
            LIMIT 20
            """
        ),
        "brand_mix": query_rows(
            conn,
            """
            SELECT
                COALESCE(m.nombre, 'Sin marca') AS marca_nombre,
                COUNT(*)::bigint AS productos,
                COALESCE(SUM(p.cantidad_stock),0)::numeric(18,2) AS stock_total
            FROM core.productos p
            LEFT JOIN core.marcas m ON m.id = p.marca_id
            GROUP BY 1
            ORDER BY productos DESC, marca_nombre
            LIMIT 20
            """
        ),
    }


def build_inventory(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        **meta,
        "filters_available": {
            "bodegas": filters["bodegas"],
            "categories": filters["categories"],
        },
        "movement_facts": query_rows(
            conn,
            """
            SELECT
                m.id,
                m.fecha::date AS date,
                m.tipo,
                COALESCE(b.nombre, 'Sin bodega') AS bodega_nombre,
                m.bodega_id,
                COALESCE(m.total,0)::numeric(18,2) AS total
            FROM core.movimientos m
            LEFT JOIN core.bodegas b ON b.id = m.bodega_id
            ORDER BY m.fecha
            """
        ),
        "movement_line_facts": query_rows(
            conn,
            """
            SELECT
                md.movimiento_id,
                m.fecha::date AS date,
                m.tipo,
                COALESCE(b.nombre, 'Sin bodega') AS bodega_nombre,
                COALESCE(p.nombre, 'Sin producto') AS producto_nombre,
                COALESCE(c.nombre, 'Sin categoría') AS categoria_nombre,
                COALESCE(md.cantidad,0)::numeric(18,2) AS cantidad,
                (COALESCE(md.cantidad,0) * COALESCE(md.costo_promedio,0))::numeric(18,2) AS costo_total
            FROM core.movimiento_detalles md
            INNER JOIN core.movimientos m ON m.id = md.movimiento_id
            LEFT JOIN core.bodegas b ON b.id = m.bodega_id
            LEFT JOIN core.productos p ON p.id = md.producto_id
            LEFT JOIN core.categorias c ON c.id = p.categoria_id
            ORDER BY m.fecha
            """
        ),
        "bodega_summary": query_rows(
            conn,
            """
            SELECT
                COALESCE(b.nombre, 'Sin bodega') AS bodega_nombre,
                COUNT(*)::bigint AS movimientos,
                COALESCE(SUM(m.total),0)::numeric(18,2) AS valor_total
            FROM core.movimientos m
            LEFT JOIN core.bodegas b ON b.id = m.bodega_id
            GROUP BY 1
            ORDER BY movimientos DESC, valor_total DESC
            LIMIT 20
            """
        ),
    }


def build_accounting(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        **meta,
        "filters_available": {
            "accounts": filters["accounts"],
            "cost_centers": filters["cost_centers"],
        },
        "monthly_summary": query_rows(
            conn,
            """
            SELECT
                date_trunc('month', a.fecha)::date AS period,
                COUNT(DISTINCT a.id)::bigint AS asientos,
                COUNT(*)::bigint AS lineas,
                COALESCE(SUM(CASE WHEN ad.tipo = 'D' THEN ad.valor ELSE 0 END),0)::numeric(18,2) AS debe,
                COALESCE(SUM(CASE WHEN ad.tipo = 'H' THEN ad.valor ELSE 0 END),0)::numeric(18,2) AS haber
            FROM core.asiento_detalles ad
            INNER JOIN core.asientos a ON a.id = ad.asiento_id
            GROUP BY 1
            ORDER BY 1
            """
        ),
        "monthly_facts": query_rows(
            conn,
            """
            SELECT
                date_trunc('month', a.fecha)::date AS period,
                ad.cuenta_id,
                COALESCE(cc.nombre, 'Sin cuenta') AS cuenta_nombre,
                ad.centro_costo_id,
                COALESCE(ct.nombre, 'Sin centro de costo') AS centro_costo_nombre,
                ad.tipo,
                COUNT(*)::bigint AS lineas,
                COALESCE(SUM(ad.valor),0)::numeric(18,2) AS valor_total
            FROM core.asiento_detalles ad
            INNER JOIN core.asientos a ON a.id = ad.asiento_id
            LEFT JOIN core.cuentas_contables cc ON cc.id = ad.cuenta_id
            LEFT JOIN core.centros_costo ct ON ct.id = ad.centro_costo_id
            GROUP BY 1, 2, 3, 4, 5, 6
            ORDER BY 1
            """
        ),
        "account_totals": query_rows(
            conn,
            """
            SELECT
                COALESCE(cc.nombre, 'Sin cuenta') AS cuenta_nombre,
                COUNT(*)::bigint AS lineas,
                COALESCE(SUM(ad.valor),0)::numeric(18,2) AS valor_total
            FROM core.asiento_detalles ad
            LEFT JOIN core.cuentas_contables cc ON cc.id = ad.cuenta_id
            GROUP BY 1
            ORDER BY valor_total DESC
            LIMIT 25
            """
        ),
        "cost_center_totals": query_rows(
            conn,
            """
            SELECT
                COALESCE(ct.nombre, 'Sin centro de costo') AS centro_costo_nombre,
                COUNT(*)::bigint AS lineas,
                COALESCE(SUM(ad.valor),0)::numeric(18,2) AS valor_total
            FROM core.asiento_detalles ad
            LEFT JOIN core.centros_costo ct ON ct.id = ad.centro_costo_id
            GROUP BY 1
            ORDER BY valor_total DESC
            LIMIT 25
            """
        ),
    }


def build_quality(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        **meta,
        "filters_available": filters,
        "source_vs_core": query_rows(
            conn,
            """
            SELECT
                er.resource,
                er.source_count,
                CASE er.resource
                    WHEN 'cuenta-contable' THEN (SELECT COUNT(*)::bigint FROM core.cuentas_contables)
                    WHEN 'categoria' THEN (SELECT COUNT(*)::bigint FROM core.categorias)
                    WHEN 'bodega' THEN (SELECT COUNT(*)::bigint FROM core.bodegas)
                    WHEN 'marca' THEN (SELECT COUNT(*)::bigint FROM core.marcas)
                    WHEN 'unidad' THEN (SELECT COUNT(*)::bigint FROM core.unidades)
                    WHEN 'centro-costo' THEN (SELECT COUNT(*)::bigint FROM core.centros_costo)
                    WHEN 'contabilidad/periodo' THEN (SELECT COUNT(*)::bigint FROM core.periodos)
                    WHEN 'persona' THEN (SELECT COUNT(*)::bigint FROM core.personas)
                    WHEN 'producto' THEN (SELECT COUNT(*)::bigint FROM core.productos)
                    WHEN 'movimiento-inventario' THEN (SELECT COUNT(*)::bigint FROM core.movimientos)
                    WHEN 'documento' THEN (SELECT COUNT(*)::bigint FROM core.documentos)
                    WHEN 'documento/tickets' THEN (SELECT COUNT(*)::bigint FROM core.tickets_documentos)
                    WHEN 'contabilidad/asiento' THEN (SELECT COUNT(*)::bigint FROM core.asientos)
                END AS core_primary_count,
                er.source_count - CASE er.resource
                    WHEN 'cuenta-contable' THEN (SELECT COUNT(*)::bigint FROM core.cuentas_contables)
                    WHEN 'categoria' THEN (SELECT COUNT(*)::bigint FROM core.categorias)
                    WHEN 'bodega' THEN (SELECT COUNT(*)::bigint FROM core.bodegas)
                    WHEN 'marca' THEN (SELECT COUNT(*)::bigint FROM core.marcas)
                    WHEN 'unidad' THEN (SELECT COUNT(*)::bigint FROM core.unidades)
                    WHEN 'centro-costo' THEN (SELECT COUNT(*)::bigint FROM core.centros_costo)
                    WHEN 'contabilidad/periodo' THEN (SELECT COUNT(*)::bigint FROM core.periodos)
                    WHEN 'persona' THEN (SELECT COUNT(*)::bigint FROM core.personas)
                    WHEN 'producto' THEN (SELECT COUNT(*)::bigint FROM core.productos)
                    WHEN 'movimiento-inventario' THEN (SELECT COUNT(*)::bigint FROM core.movimientos)
                    WHEN 'documento' THEN (SELECT COUNT(*)::bigint FROM core.documentos)
                    WHEN 'documento/tickets' THEN (SELECT COUNT(*)::bigint FROM core.tickets_documentos)
                    WHEN 'contabilidad/asiento' THEN (SELECT COUNT(*)::bigint FROM core.asientos)
                END AS difference
            FROM meta.extract_runs er
            WHERE er.run_id = (SELECT max(run_id) FROM meta.extract_runs)
            ORDER BY er.started_at, er.resource
            """
        ),
        "fk_health": query_rows(conn, "SELECT relation_name, orphan_count FROM reporting.v_fk_health ORDER BY relation_name"),
        "temporal_coverage": query_rows(conn, "SELECT resource, row_count, min_date, max_date FROM reporting.v_temporal_coverage ORDER BY resource"),
        "placeholders": query_rows(
            conn,
            """
            SELECT 'categorias' AS table_name, COUNT(*)::bigint AS placeholder_count FROM core.categorias WHERE nombre LIKE '__missing__:%'
            UNION ALL SELECT 'cuentas_contables', COUNT(*)::bigint FROM core.cuentas_contables WHERE nombre LIKE '__missing__:%'
            UNION ALL SELECT 'personas', COUNT(*)::bigint FROM core.personas WHERE razon_social LIKE '__missing__:%'
            UNION ALL SELECT 'productos', COUNT(*)::bigint FROM core.productos WHERE nombre LIKE '__missing__:%'
            UNION ALL SELECT 'documentos', COUNT(*)::bigint FROM core.documentos WHERE documento LIKE '__missing__:%'
            """
        ),
        "nulls_allowed": query_rows(
            conn,
            """
            SELECT 'documento_detalles_producto_id_null' AS metric, COUNT(*)::bigint AS value FROM core.documento_detalles WHERE producto_id IS NULL
            UNION ALL SELECT 'tickets_detalles_producto_id_null', COUNT(*)::bigint FROM core.tickets_detalles WHERE producto_id IS NULL
            UNION ALL SELECT 'documentos_sin_persona_id', COUNT(*)::bigint FROM core.documentos WHERE persona_id IS NULL
            """
        ),
    }


def build_consistency_review(conn) -> dict[str, list[dict[str, Any]]]:
    inventory_cards = [
        {
            "area": "inventario",
            "severity": "high",
            "title": "Movimientos sin detalle",
            "metric": query_value(
                conn,
                """
                SELECT COUNT(*)::bigint
                FROM core.movimientos m
                LEFT JOIN core.movimiento_detalles d ON d.movimiento_id = m.id
                WHERE d.movimiento_id IS NULL
                """,
                default=0,
            ),
            "issue": "Existen cabeceras de movimiento sin lineas asociadas, por lo que el evento no tiene sustento operativo completo.",
            "impact": "El kardex y los analisis por producto o bodega pueden quedar incompletos o sesgados.",
            "analysis_risk": "Infla el conteo de eventos operativos y contamina cualquier lectura de rotacion, consumo o intensidad por bodega.",
            "decision_risk": "Puede llevar a sobrerreaccionar en reabastecimiento, auditoria interna o control de perdidas usando eventos que no tienen soporte real.",
            "suggested_action": "Revisar si son anulaciones, cargas parciales o errores de integracion. Excluirlos del analisis operativo o reconstruir los detalles antes de consolidar.",
            "positive_outlook": "Al depurar estas cabeceras, la lectura del inventario queda mas confiable y las decisiones de abastecimiento descansan sobre eventos completos.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT m.id, m.fecha::date AS fecha, COALESCE(m.tipo, '(sin tipo)') AS tipo, COALESCE(m.total, 0)::numeric(18,2) AS total
                FROM core.movimientos m
                LEFT JOIN core.movimiento_detalles d ON d.movimiento_id = m.id
                WHERE d.movimiento_id IS NULL
                ORDER BY m.fecha DESC
                LIMIT 5
                """,
            ),
        },
        {
            "area": "inventario",
            "severity": "medium",
            "title": "Movimientos con cantidad y total cero",
            "metric": query_value(
                conn,
                """
                SELECT COUNT(*)::bigint
                FROM (
                    SELECT m.id
                    FROM core.movimientos m
                    JOIN core.movimiento_detalles d ON d.movimiento_id = m.id
                    WHERE COALESCE(m.total, 0) = 0
                    GROUP BY m.id
                    HAVING COALESCE(SUM(d.cantidad), 0) <> 0
                ) base
                """,
                default=0,
            ),
            "issue": "Hay movimientos con detalle fisico y total monetario en cero.",
            "impact": "La valorizacion de inventario y el costo movilizado quedan subestimados o invisibles.",
            "analysis_risk": "Los analisis economicos por categoria, producto o periodo omiten costo movilizado real aunque si exista movimiento fisico.",
            "decision_risk": "Puede aparentar que ciertas entradas no cuestan nada y sesgar decisiones de precio, margen o priorizacion de compras.",
            "suggested_action": "Validar costo unitario, politica de ingresos a costo cero y recalcular el total del movimiento cuando corresponda.",
            "positive_outlook": "Corregir estos casos mejora el enlace entre inventario fisico y valor economico, lo que fortalece analisis de margen y costo.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT m.id, m.fecha::date AS fecha, COALESCE(m.tipo, '(sin tipo)') AS tipo,
                       COALESCE(SUM(d.cantidad), 0)::numeric(18,2) AS cantidad_total,
                       COALESCE(m.total, 0)::numeric(18,2) AS total
                FROM core.movimientos m
                JOIN core.movimiento_detalles d ON d.movimiento_id = m.id
                WHERE COALESCE(m.total, 0) = 0
                GROUP BY m.id, m.fecha, m.tipo, m.total
                HAVING COALESCE(SUM(d.cantidad), 0) <> 0
                ORDER BY m.fecha DESC
                LIMIT 5
                """,
            ),
        },
        {
            "area": "inventario",
            "severity": "high",
            "title": "Movimientos sin cuenta contable",
            "metric": query_value(conn, "SELECT COUNT(*)::bigint FROM core.movimientos WHERE cuenta_id IS NULL", default=0),
            "issue": "Hay movimientos de inventario que no quedaron conectados a una cuenta contable.",
            "impact": "Se debilita el puente entre operacion fisica y lectura contable del inventario.",
            "analysis_risk": "No es posible reconciliar con precision ciertos movimientos contra la estructura contable ni trazar impacto economico por movimiento.",
            "decision_risk": "Dificulta decisiones sobre costo, rentabilidad y control de inventario porque parte del flujo queda fuera del mapa contable.",
            "suggested_action": "Completar el mapeo de cuenta a nivel de movimiento o reforzar la herencia contable desde producto/categoria al momento de registrar el evento.",
            "positive_outlook": "Conectar estos movimientos a cuenta mejora la conciliacion inventario-contabilidad y permite analisis mas confiables de costo y margen.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT id, fecha::date AS fecha, COALESCE(tipo, '(sin tipo)') AS tipo, bodega_id
                FROM core.movimientos
                WHERE cuenta_id IS NULL
                ORDER BY fecha DESC
                LIMIT 5
                """,
            ),
        },
        {
            "area": "inventario",
            "severity": "high",
            "title": "Productos con stock negativo",
            "metric": query_value(conn, "SELECT COUNT(*)::bigint FROM core.productos WHERE cantidad_stock < 0", default=0),
            "issue": "Existen productos con saldo de stock menor que cero.",
            "impact": "Rompe consistencia de inventario, puede esconder faltantes fisicos o movimientos no registrados.",
            "analysis_risk": "Toda lectura de stock disponible, cobertura o rotacion queda contaminada para esos productos.",
            "decision_risk": "Puede inducir compras urgentes innecesarias o esconder un problema de registro y control fisico.",
            "suggested_action": "Revisar el kardex del producto, regularizar entradas y salidas pendientes y bloquear analisis de rotacion hasta corregir el saldo.",
            "positive_outlook": "Regularizar estos saldos fortalece la confianza en stock disponible y mejora la planeacion de reposicion.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT id, nombre, cantidad_stock::numeric(18,2) AS cantidad_stock
                FROM core.productos
                WHERE cantidad_stock < 0
                ORDER BY cantidad_stock ASC, nombre
                LIMIT 5
                """,
            ),
        },
        {
            "area": "inventario",
            "severity": "low",
            "title": "Productos sin marca",
            "metric": query_value(conn, "SELECT COUNT(*)::bigint FROM core.productos WHERE marca_id IS NULL", default=0),
            "issue": "Una porcion importante del catalogo no tiene marca asociada.",
            "impact": "El analisis por proveedor, fabricante o linea de marca queda incompleto.",
            "analysis_risk": "Se pierde profundidad al segmentar catalogo, mix y comportamiento comercial por marca.",
            "decision_risk": "Reduce la calidad de decisiones comerciales y de compras cuando se requiere priorizar marcas o fabricantes.",
            "suggested_action": "Completar la gobernanza del catalogo y marcar como opcional solo los productos que realmente no usan marca.",
            "positive_outlook": "Completar este atributo abre una segmentacion mas rica para compras, ventas y seguimiento del portafolio.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT id, nombre, categoria_id
                FROM core.productos
                WHERE marca_id IS NULL
                ORDER BY nombre
                LIMIT 5
                """,
            ),
        },
    ]
    accounting_cards = [
        {
            "area": "cuenta_contable",
            "severity": "high",
            "title": "Productos sin cuenta de compra",
            "metric": query_value(conn, "SELECT COUNT(*)::bigint FROM core.productos WHERE cuenta_compra_id IS NULL", default=0),
            "issue": "Hay productos sin mapeo de cuenta de compra.",
            "impact": "Las compras pueden quedar sin clasificacion correcta o depender de imputaciones manuales.",
            "analysis_risk": "El costo de adquisicion por linea de producto pierde trazabilidad contable y complica cortes por categoria.",
            "decision_risk": "Puede sesgar decisiones de compra, margen y control presupuestario por no saber donde cae realmente el gasto.",
            "suggested_action": "Asignar cuenta de compra por producto o heredarla desde categoria cuando aplique.",
            "positive_outlook": "Cerrar este mapeo mejora la lectura de compras y deja mejor preparado el modelo para conciliacion contable automatica.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT id, nombre, categoria_id
                FROM core.productos
                WHERE cuenta_compra_id IS NULL
                ORDER BY nombre
                LIMIT 5
                """,
            ),
        },
        {
            "area": "cuenta_contable",
            "severity": "high",
            "title": "Productos sin cuenta de costo",
            "metric": query_value(conn, "SELECT COUNT(*)::bigint FROM core.productos WHERE cuenta_costo_id IS NULL", default=0),
            "issue": "Hay productos sin cuenta de costo asociada.",
            "impact": "El analisis de margen y la salida contable de inventario pueden quedar incompletos.",
            "analysis_risk": "La lectura de rentabilidad por producto o categoria queda debilitada porque el costo no aterriza de forma consistente.",
            "decision_risk": "Puede llevar a fijar precios o promociones sin un costo bien representado en el modelo.",
            "suggested_action": "Completar la cuenta de costo a nivel de producto o definir una regla de herencia desde la categoria.",
            "positive_outlook": "Con este ajuste el dashboard puede evolucionar hacia analisis de margen real por producto con mucha mas confianza.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT id, nombre, categoria_id
                FROM core.productos
                WHERE cuenta_costo_id IS NULL
                ORDER BY nombre
                LIMIT 5
                """,
            ),
        },
        {
            "area": "cuenta_contable",
            "severity": "high",
            "title": "Categorias con mapeo contable incompleto",
            "metric": query_value(
                conn,
                """
                SELECT COUNT(*)::bigint
                FROM core.categorias
                WHERE cuenta_venta IS NULL OR cuenta_compra IS NULL OR cuenta_inventario IS NULL
                """,
                default=0,
            ),
            "issue": "Hay categorias sin todas las cuentas clave de venta, compra o inventario.",
            "impact": "Los nuevos productos o documentos que dependan de la categoria pueden heredar configuracion incompleta.",
            "analysis_risk": "La consistencia futura del modelo se erosiona porque cada nuevo dato que herede desde la categoria puede nacer incompleto.",
            "decision_risk": "Aumenta el riesgo de clasificacion desigual entre productos similares y distorsiona comparaciones entre lineas.",
            "suggested_action": "Completar el mapeo por categoria y separar categorias de servicio de categorias inventariables para no mezclar reglas.",
            "positive_outlook": "Corregir la capa categoria reduce errores futuros y simplifica la gobernanza contable del catalogo.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT id, nombre, cuenta_venta, cuenta_compra, cuenta_inventario
                FROM core.categorias
                WHERE cuenta_venta IS NULL OR cuenta_compra IS NULL OR cuenta_inventario IS NULL
                ORDER BY nombre
                LIMIT 5
                """,
            ),
        },
        {
            "area": "cuenta_contable",
            "severity": "medium",
            "title": "Lineas directas sin producto y con cuenta",
            "metric": query_value(
                conn,
                """
                SELECT COUNT(*)::bigint
                FROM core.documento_detalles
                WHERE producto_id IS NULL AND cuenta_id IS NOT NULL
                """,
                default=0,
            ),
            "issue": "Existen lineas documentales imputadas directo a cuenta contable sin producto asociado.",
            "impact": "Estas lineas no deben mezclarse con analisis de inventario, pero si con revision de ingresos o gastos directos.",
            "analysis_risk": "Si se leen como venta de producto, inflan o deforman mix comercial, top productos y analisis de inventario.",
            "decision_risk": "Puede llevar a decisiones equivocadas sobre portafolio si cargos directos o servicios se interpretan como producto fisico.",
            "suggested_action": "Separarlas explicitamente como servicios o cargos directos y revisar si algunas debieron codificarse como producto.",
            "positive_outlook": "Separar bien estos casos mejora la lectura entre ingreso directo, servicio e inventario, lo que hace el analisis mucho mas fino.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT documento_id, detalle_index, cuenta_id, producto_id,
                       cantidad::numeric(18,2) AS cantidad, precio::numeric(18,2) AS precio
                FROM core.documento_detalles
                WHERE producto_id IS NULL AND cuenta_id IS NOT NULL
                ORDER BY documento_id DESC, detalle_index DESC
                LIMIT 5
                """,
            ),
        },
        {
            "area": "cuenta_contable",
            "severity": "low",
            "title": "Cuentas contables sin uso historico",
            "metric": query_value(
                conn,
                """
                SELECT COUNT(*)::bigint
                FROM core.cuentas_contables c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM core.asiento_detalles d
                    WHERE d.cuenta_id = c.id
                )
                """,
                default=0,
            ),
            "issue": "El plan contable contiene cuentas sin movimiento dentro del historico cargado.",
            "impact": "No rompe integridad, pero complica revision manual y puede ocultar catalogo obsoleto.",
            "analysis_risk": "Amplia el ruido del catalogo contable y dificulta distinguir cuentas realmente operativas de cuentas residuales.",
            "decision_risk": "Puede distraer esfuerzos de saneamiento y llevar a sobredimensionar la complejidad operativa del plan contable.",
            "suggested_action": "Depurar el plan contable operativo o clasificar cuentas vigentes sin uso para no tratarlas como anomalias futuras.",
            "positive_outlook": "Un plan contable mas limpio acelera revision, capacitacion y analisis por cuenta realmente usada.",
            "sample_rows": query_rows(
                conn,
                """
                SELECT id, nombre, codigo
                FROM core.cuentas_contables c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM core.asiento_detalles d
                    WHERE d.cuenta_id = c.id
                )
                ORDER BY nombre
                LIMIT 5
                """,
            ),
        },
    ]
    return {
        "inventory": [card for card in inventory_cards if int(card["metric"] or 0) > 0],
        "accounting": [card for card in accounting_cards if int(card["metric"] or 0) > 0],
    }


def build_source_overview(conn, meta: dict[str, Any], recent_runs: list[dict[str, Any]], summary: dict[str, Any]) -> dict[str, Any]:
    history_summary = query_rows(
        conn,
        """
        WITH run_summary AS (
            SELECT
                run_id,
                CASE
                    WHEN SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) > 0 THEN 'error'
                    WHEN SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) = COUNT(*) THEN 'success'
                    ELSE 'partial'
                END AS run_status
            FROM meta.extract_runs
            GROUP BY run_id
        )
        SELECT
            COUNT(*)::bigint AS total_runs,
            SUM(CASE WHEN run_status = 'success' THEN 1 ELSE 0 END)::bigint AS successful_runs,
            SUM(CASE WHEN run_status = 'error' THEN 1 ELSE 0 END)::bigint AS failed_runs
        FROM run_summary
        """
    )[0]
    latest_success = next((row for row in recent_runs if row.get("status") == "success"), None)
    latest_error = next((row for row in recent_runs if row.get("status") == "error"), None)
    senior_summary = [
        {
            "title": "Lectura senior del estado actual",
            "body": f"La base publicada al dashboard sigue siendo local y materializada, pero su fuente de origen real es Contifico API. Hoy el snapshot vigente parte del run {meta.get('run_id')} y refleja {summary.get('resources_processed', 0)} recursos procesados con {summary.get('tables_updated', 0)} tablas impactadas.",
        },
        {
            "title": "Cadena de origen de los datos",
            "body": "La secuencia operativa es: Contifico API -> contifico_pg_backfill.py -> PostgreSQL contifico_backfill -> export_dashboard_data.py -> dashboard/data/*.json -> vista estatica del dashboard. La API local solo orquesta estado y refresh, no reemplaza la base maestra.",
        },
        {
            "title": "Modo local versus modo vivo",
            "body": "El modo local mantiene estabilidad porque sirve el ultimo snapshot ya consolidado. El modo vivo ahora separa refresh rapido y refresh completo: el rapido lee cambios recientes y el completo relee todo el historico para reconciliacion profunda.",
        },
        {
            "title": "Historial reciente del pipeline",
            "body": f"El historial acumulado registra {history_summary.get('total_runs', 0)} corridas, de las cuales {history_summary.get('successful_runs', 0)} fueron exitosas y {history_summary.get('failed_runs', 0)} cerraron con error. La ultima corrida exitosa es {latest_success.get('run_id') if latest_success else '--'} y la ultima corrida con error es {latest_error.get('run_id') if latest_error else '--'}.",
        },
    ]
    source_chain = [
        {
            "layer": "Contifico API",
            "role": "Fuente viva de personas, productos, documentos, tickets, movimientos, asientos y catalogos.",
        },
        {
            "layer": "contifico_pg_backfill.py",
            "role": "Extrae, deduplica, normaliza y carga el historico en PostgreSQL.",
        },
        {
            "layer": "PostgreSQL contifico_backfill",
            "role": "Base maestra local para analitica, revision tecnica y trazabilidad.",
        },
        {
            "layer": "export_dashboard_data.py",
            "role": "Materializa snapshots JSON optimizados para navegacion web.",
        },
        {
            "layer": "dashboard/data/*.json",
            "role": "Snapshot local estable consumido por la vista analitica.",
        },
        {
            "layer": "local_dashboard_api.py",
            "role": "Expone estado tecnico y orquesta refresh bajo demanda desde la UI.",
        },
    ]
    operating_modes = [
        {
            "mode": "Local estable",
            "script": "dashboard/start_dashboard_server.ps1",
            "description": "Levanta dashboard + API tecnica y conserva el ultimo snapshot materializado sin ejecutar refresco automatico.",
        },
        {
            "mode": "Refresh rapido",
            "script": "dashboard/start_dashboard_live_refresh.ps1",
            "description": "Levanta dashboard + API tecnica y dispara el modo operacional: lee cambios recientes, actualiza IDs impactados y republica el snapshot.",
        },
        {
            "mode": "Refresh completo",
            "script": "dashboard/start_dashboard_full_refresh.ps1",
            "description": "Levanta dashboard + API tecnica y dispara un backfill historico completo desde Contifico API hacia PostgreSQL y JSON.",
        },
    ]
    return {
        "senior_summary": senior_summary,
        "source_chain": source_chain,
        "operating_modes": operating_modes,
        "history_summary": {
            **history_summary,
            "latest_success_run": latest_success.get("run_id") if latest_success else None,
            "latest_success_finished_at": latest_success.get("finished_at") if latest_success else None,
            "latest_error_run": latest_error.get("run_id") if latest_error else None,
            "latest_error_finished_at": latest_error.get("finished_at") if latest_error else None,
        },
    }


def build_priority_matrix(consistency_review: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    priority_band_map = {
        "P1": "alto",
        "P2": "medio",
        "P3": "bajo",
    }
    priority_rules = {
        "Categorias con mapeo contable incompleto": {
            "priority_rank": 1,
            "priority_label": "P1",
            "responsible_role": "Contabilidad + Maestro de catalogo",
            "workstream": "Gobernanza contable base",
            "why_now": "Corrige la capa de herencia antes de seguir ajustando productos y transacciones.",
            "success_criteria": "Toda categoria inventariable queda con cuenta de venta, compra e inventario completas.",
        },
        "Productos sin cuenta de costo": {
            "priority_rank": 2,
            "priority_label": "P1",
            "responsible_role": "Contabilidad + Responsable de productos",
            "workstream": "Rentabilidad y costo",
            "why_now": "Sin cuenta de costo no hay margen confiable por producto ni salida contable consistente.",
            "success_criteria": "Todo producto activo queda con cuenta de costo valida o heredada con regla explicita.",
        },
        "Productos sin cuenta de compra": {
            "priority_rank": 3,
            "priority_label": "P1",
            "responsible_role": "Compras + Contabilidad",
            "workstream": "Clasificacion de adquisiciones",
            "why_now": "Evita que nuevas compras sigan entrando sin una imputacion consistente.",
            "success_criteria": "Todo producto comprable queda vinculado a cuenta de compra o regla de herencia controlada.",
        },
        "Movimientos sin cuenta contable": {
            "priority_rank": 4,
            "priority_label": "P1",
            "responsible_role": "Inventario + Contabilidad",
            "workstream": "Conciliacion operacion-contabilidad",
            "why_now": "Cierra el puente entre movimiento fisico y lectura contable antes de profundizar analisis de costo.",
            "success_criteria": "Todo movimiento relevante queda trazable a una cuenta contable o justificado como excepcion controlada.",
        },
        "Movimientos sin detalle": {
            "priority_rank": 5,
            "priority_label": "P1",
            "responsible_role": "Operaciones + Inventario",
            "workstream": "Calidad transaccional",
            "why_now": "El evento sin detalle degrada kardex, rotacion y auditoria operativa.",
            "success_criteria": "Toda cabecera valida queda con detalle asociado o es marcada como anulada/no analizable.",
        },
        "Productos con stock negativo": {
            "priority_rank": 6,
            "priority_label": "P1",
            "responsible_role": "Inventario + Bodega",
            "workstream": "Regularizacion de saldos",
            "why_now": "El stock negativo rompe disponibilidad, reposicion y control fisico.",
            "success_criteria": "Los productos con saldo negativo quedan regularizados y monitoreados.",
        },
        "Movimientos con cantidad y total cero": {
            "priority_rank": 7,
            "priority_label": "P2",
            "responsible_role": "Inventario + Costos",
            "workstream": "Valorizacion de movimientos",
            "why_now": "Impacta la lectura economica del inventario aun cuando el movimiento fisico ya exista.",
            "success_criteria": "Todo movimiento con cantidad relevante tiene una valorizacion consistente o una excepcion documentada.",
        },
        "Lineas directas sin producto y con cuenta": {
            "priority_rank": 8,
            "priority_label": "P2",
            "responsible_role": "Comercial + Contabilidad",
            "workstream": "Separacion producto-servicio",
            "why_now": "Necesita limpiarse antes de interpretar mix comercial y top productos.",
            "success_criteria": "Las lineas directas quedan clasificadas como servicio/cargo o migradas a producto cuando aplique.",
        },
        "Productos sin marca": {
            "priority_rank": 9,
            "priority_label": "P3",
            "responsible_role": "Maestro de productos + Compras",
            "workstream": "Enriquecimiento del catalogo",
            "why_now": "No rompe integridad, pero limita segmentacion y lectura de portafolio.",
            "success_criteria": "Los productos que usan marca quedan clasificados y los que no, marcados como excepcion valida.",
        },
        "Cuentas contables sin uso historico": {
            "priority_rank": 10,
            "priority_label": "P3",
            "responsible_role": "Contabilidad",
            "workstream": "Depuracion del plan contable",
            "why_now": "Es limpieza de catalogo, util despues de estabilizar las capas transaccionales.",
            "success_criteria": "El plan contable distingue cuentas operativas, historicas y obsoletas con criterio explicito.",
        },
    }
    all_cards = [*consistency_review.get("inventory", []), *consistency_review.get("accounting", [])]
    matrix: list[dict[str, Any]] = []
    for card in all_cards:
        rule = priority_rules.get(card["title"], {})
        matrix.append(
            {
                "priority_rank": rule.get("priority_rank", 999),
                "correction_order": rule.get("priority_rank", 999),
                "priority_code": rule.get("priority_label", "P3"),
                "priority_level": priority_band_map.get(rule.get("priority_label", "P3"), "bajo"),
                "severity": card.get("severity"),
                "title": card.get("title"),
                "area": card.get("area"),
                "metric": card.get("metric"),
                "recommended_owner": rule.get("responsible_role", "Equipo de datos"),
                "workstream": rule.get("workstream", "Saneamiento de calidad"),
                "why_now": rule.get("why_now", card.get("impact")),
                "success_criteria": rule.get("success_criteria", card.get("suggested_action")),
            }
        )
    matrix.sort(key=lambda row: (row["priority_rank"], row["title"]))
    return matrix


def build_technical(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    source_vs_core = query_rows(
        conn,
        """
        SELECT resource, source_count, core_count, (source_count - core_count)::bigint AS difference
        FROM (
            SELECT
                er.resource,
                max(er.source_count)::bigint AS source_count,
                CASE er.resource
                    WHEN 'persona' THEN (SELECT COUNT(*) FROM core.personas)
                    WHEN 'producto' THEN (SELECT COUNT(*) FROM core.productos)
                    WHEN 'movimiento-inventario' THEN (SELECT COUNT(*) FROM core.movimientos)
                    WHEN 'documento' THEN (SELECT COUNT(*) FROM core.documentos)
                    WHEN 'documento/tickets' THEN (SELECT COUNT(*) FROM core.tickets_documentos)
                    WHEN 'contabilidad/asiento' THEN (SELECT COUNT(*) FROM core.asientos)
                    WHEN 'contabilidad/periodo' THEN (SELECT COUNT(*) FROM core.periodos)
                    WHEN 'categoria' THEN (SELECT COUNT(*) FROM core.categorias)
                    WHEN 'bodega' THEN (SELECT COUNT(*) FROM core.bodegas)
                    WHEN 'marca' THEN (SELECT COUNT(*) FROM core.marcas)
                    WHEN 'unidad' THEN (SELECT COUNT(*) FROM core.unidades)
                    WHEN 'cuenta-contable' THEN (SELECT COUNT(*) FROM core.cuentas_contables)
                    WHEN 'centro-costo' THEN (SELECT COUNT(*) FROM core.centros_costo)
                    ELSE 0
                END::bigint AS core_count
            FROM meta.extract_runs er
            GROUP BY er.resource
        ) base
        ORDER BY difference DESC, resource
        """,
    )
    fk_health = query_rows(conn, "SELECT relation_name, orphan_count FROM reporting.v_fk_health ORDER BY relation_name")
    placeholders = query_rows(
        conn,
        """
        SELECT 'categorias' AS table_name, COUNT(*)::bigint AS placeholder_count FROM core.categorias WHERE nombre LIKE '__missing__:%'
        UNION ALL SELECT 'productos', COUNT(*)::bigint FROM core.productos WHERE nombre LIKE '__missing__:%'
        UNION ALL SELECT 'personas', COUNT(*)::bigint FROM core.personas WHERE razon_social LIKE '__missing__:%'
        UNION ALL SELECT 'documentos', COUNT(*)::bigint FROM core.documentos WHERE documento LIKE '__missing__:%'
        """
    )
    nulls_allowed = query_rows(
        conn,
        """
        SELECT 'documento_detalles_producto_id_null' AS metric, COUNT(*)::bigint AS value FROM core.documento_detalles WHERE producto_id IS NULL
        UNION ALL SELECT 'tickets_detalles_producto_id_null', COUNT(*)::bigint FROM core.tickets_detalles WHERE producto_id IS NULL
        UNION ALL SELECT 'documentos_sin_persona_id', COUNT(*)::bigint FROM core.documentos WHERE persona_id IS NULL
        """
    )
    consistency_review = build_consistency_review(conn)
    recent_runs = query_rows(
        conn,
        """
        SELECT
            run_id,
            min(started_at) AS started_at,
            max(finished_at) AS finished_at,
            COALESCE(EXTRACT(EPOCH FROM (max(finished_at) - min(started_at)))::bigint, 0) AS duration_seconds,
            COUNT(*)::integer AS resources_processed,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)::integer AS resources_success,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)::integer AS resources_failed,
            SUM(source_count)::bigint AS source_rows,
            SUM(raw_row_count)::bigint AS raw_rows,
            CASE
                WHEN SUM(CASE WHEN mode = 'backfill' THEN 1 ELSE 0 END) > 0 THEN 'backfill'
                WHEN SUM(CASE WHEN mode = 'refresh' THEN 1 ELSE 0 END) > 0 THEN 'refresh'
                ELSE COALESCE(MIN(mode), 'desconocido')
            END AS run_mode,
            CASE
                WHEN SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) > 0 THEN 'error'
                WHEN SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) = COUNT(*) THEN 'success'
                ELSE 'partial'
            END AS status
        FROM meta.extract_runs
        GROUP BY run_id
        ORDER BY run_id DESC
        LIMIT 12
        """,
    )
    last_run = recent_runs[0] if recent_runs else {}
    latest_run_id = last_run.get("run_id") or meta.get("run_id")
    historical_core_rows_stored = query_value(
        conn,
        """
        SELECT
            (SELECT COUNT(*) FROM core.categorias)
          + (SELECT COUNT(*) FROM core.bodegas)
          + (SELECT COUNT(*) FROM core.marcas)
          + (SELECT COUNT(*) FROM core.unidades)
          + (SELECT COUNT(*) FROM core.cuentas_contables)
          + (SELECT COUNT(*) FROM core.centros_costo)
          + (SELECT COUNT(*) FROM core.periodos)
          + (SELECT COUNT(*) FROM core.personas)
          + (SELECT COUNT(*) FROM core.productos)
          + (SELECT COUNT(*) FROM core.movimientos)
          + (SELECT COUNT(*) FROM core.movimiento_detalles)
          + (SELECT COUNT(*) FROM core.documentos)
          + (SELECT COUNT(*) FROM core.documento_detalles)
          + (SELECT COUNT(*) FROM core.documento_cobros)
          + (SELECT COUNT(*) FROM core.tickets_documentos)
          + (SELECT COUNT(*) FROM core.tickets_detalles)
          + (SELECT COUNT(*) FROM core.tickets_items)
          + (SELECT COUNT(*) FROM core.asientos)
          + (SELECT COUNT(*) FROM core.asiento_detalles)
          AS total_core_rows
        """,
        default=0,
    )
    resource_metrics = (
        query_rows(
            conn,
            """
            SELECT
                er.resource,
                er.status,
                er.pages_fetched,
                er.source_count,
                er.raw_row_count,
                er.started_at,
                er.finished_at,
                COALESCE(EXTRACT(EPOCH FROM (er.finished_at - er.started_at))::bigint, 0) AS duration_seconds,
                COALESCE((
                    SELECT SUM(lm.row_count)::bigint
                    FROM meta.load_metrics lm
                    WHERE lm.run_id = er.run_id
                      AND lm.resource = er.resource
                      AND lm.stage = 'core'
                ), 0) AS core_rows_loaded,
                er.table_counts_jsonb
            FROM meta.extract_runs er
            WHERE er.run_id = %s
            ORDER BY er.resource
            """,
            (latest_run_id,),
        )
        if latest_run_id
        else []
    )
    load_metrics = (
        query_rows(
            conn,
            """
            SELECT stage, table_name, SUM(row_count)::bigint AS row_count, MAX(measured_at) AS measured_at
            FROM meta.load_metrics
            WHERE run_id = %s
            GROUP BY stage, table_name
            ORDER BY stage, row_count DESC, table_name
            """,
            (latest_run_id,),
        )
        if latest_run_id
        else []
    )
    watermarks = query_rows(
        conn,
        """
        SELECT resource, last_run_id, min_record_date, max_record_date, updated_at
        FROM meta.watermarks
        ORDER BY resource
        """
    )
    coverage_rows = query_rows(conn, "SELECT resource, row_count, min_date, max_date FROM reporting.v_temporal_coverage ORDER BY resource")
    updated_tables = len({row["table_name"] for row in load_metrics})
    resources_success = sum(1 for row in resource_metrics if row.get("status") == "success")
    resources_failed = sum(1 for row in resource_metrics if row.get("status") == "failed")
    total_core_rows = sum(int(row.get("row_count") or 0) for row in load_metrics if row.get("stage") == "core")
    orphan_total = sum(int(row.get("orphan_count") or 0) for row in fk_health)
    movement_anomaly = next((row for row in source_vs_core if row.get("resource") == "movimiento-inventario"), None)
    last_run_mode = last_run.get("run_mode") or "desconocido"
    last_run_mode_label = "Refresh completo" if last_run_mode == "backfill" else "Refresh rapido" if last_run_mode == "refresh" else "Modo desconocido"
    read_scope_note = (
        "Lee cambios recientes e IDs impactados; mantiene el historico ya almacenado."
        if last_run_mode == "refresh"
        else "Relee todo el historico y reconstruye completamente la base local."
        if last_run_mode == "backfill"
        else "No hay suficiente informacion para clasificar el alcance de la corrida."
    )
    generated_at_value = meta.get("generated_at")
    generated_at_dt = None
    if isinstance(generated_at_value, dt.datetime):
        generated_at_dt = generated_at_value
    elif isinstance(generated_at_value, str):
        generated_at_dt = dt.datetime.fromisoformat(generated_at_value)
    freshness_seconds = None
    if generated_at_dt:
        freshness_seconds = int((dt.datetime.now(generated_at_dt.tzinfo or dt.timezone.utc) - generated_at_dt).total_seconds())
    alerts = [
        {
            "level": "warning" if movement_anomaly and movement_anomaly.get("difference") else "info",
            "title": "Diferencia de movimientos",
            "message": "Fuente vs core en movimiento-inventario despues de la normalizacion por ID unico.",
            "metric": int(movement_anomaly.get("difference") or 0) if movement_anomaly else 0,
        },
        {
            "level": "info" if orphan_total == 0 else "warning",
            "title": "Salud relacional",
            "message": "Conteo consolidado de huerfanos en relaciones validadas del modelo PostgreSQL.",
            "metric": orphan_total,
        },
        {
            "level": "info",
            "title": "Placeholders activos",
            "message": "Registros placeholder usados para sostener integridad referencial en dimensiones incompletas.",
            "metric": sum(int(row.get("placeholder_count") or 0) for row in placeholders),
        },
    ]
    narrative = [
        f"El snapshot tecnico vigente corresponde al run_id {latest_run_id or 'sin_corrida'} y cubre desde {meta.get('coverage_min')} hasta {meta.get('coverage_max')}.",
        f"La ultima corrida fue {last_run_mode_label.lower()}, proceso {int(last_run.get('resources_processed') or 0)} recursos, actualizo {updated_tables} tablas y materializo {total_core_rows} filas core.",
        f"Las filas leidas en esta corrida fueron {int(last_run.get('source_rows') or 0)}, mientras que la base mantiene {int(historical_core_rows_stored or 0)} filas historicas en tablas core.",
        f"La diferencia fuente vs core mas visible sigue en movimiento-inventario con {int(movement_anomaly.get('difference') or 0) if movement_anomaly else 0} filas de brecha despues de deduplicacion.",
        f"La salud referencial consolidada registra {orphan_total} huerfanos y {sum(int(row.get('placeholder_count') or 0) for row in placeholders)} placeholders controlados.",
    ]
    refresh_guidance = [
        {
            "title": "Filas leidas en esta corrida",
            "body": f"El valor {int(last_run.get('source_rows') or 0)} mide lo que el pipeline leyo hoy desde la API. Puede bajar cuando el refresh deja de releer historico completo y pasa a leer solo cambios recientes.",
        },
        {
            "title": "Historico almacenado",
            "body": f"La base local conserva {int(historical_core_rows_stored or 0)} filas en tablas core. Esa cifra representa lo que ya esta persistido y disponible para analitica, aunque la corrida actual haya leido menos filas desde origen.",
        },
        {
            "title": "Garantia del refresh rapido",
            "body": "El modo rapido captura nuevos registros y actualizaciones recientes en los recursos optimizados. Es el modo operacional recomendado para el dia a dia porque reduce tiempo sin vaciar el historico local.",
        },
        {
            "title": "Cuando usar refresh completo",
            "body": "El modo completo relee todo el historico y reconstruye la base local. Conviene para conciliacion profunda, cierres, validaciones de integridad o cuando se sospechan cambios historicos fuera de la ventana reciente.",
        },
    ]
    summary = {
        "status": last_run.get("status", "unknown"),
        "run_mode": last_run_mode,
        "run_mode_label": last_run_mode_label,
        "read_scope_note": read_scope_note,
        "resources_processed": int(last_run.get("resources_processed") or 0),
        "resources_success": resources_success,
        "resources_failed": resources_failed,
        "tables_updated": updated_tables,
        "core_rows_updated": total_core_rows,
        "source_rows_processed": int(last_run.get("source_rows") or 0),
        "raw_rows_processed": int(last_run.get("raw_rows") or 0),
        "historical_core_rows_stored": int(historical_core_rows_stored or 0),
    }
    source_overview = build_source_overview(conn, meta, recent_runs, summary)
    priority_matrix = build_priority_matrix(consistency_review)
    return {
        **meta,
        "filters_available": filters,
        "generated_at": meta.get("generated_at"),
        "run_id": latest_run_id,
        "last_refresh_started_at": last_run.get("started_at"),
        "last_refresh_finished_at": last_run.get("finished_at"),
        "last_refresh_duration_seconds": int(last_run.get("duration_seconds") or 0),
        "freshness_seconds": freshness_seconds,
        "coverage_min": meta.get("coverage_min"),
        "coverage_max": meta.get("coverage_max"),
        "summary": summary,
        "refresh_guidance": refresh_guidance,
        "resource_metrics": resource_metrics,
        "load_metrics": load_metrics,
        "watermarks": watermarks,
        "temporal_coverage": coverage_rows,
        "alerts": alerts,
        "fk_health": fk_health,
        "source_vs_core": source_vs_core,
        "placeholders": placeholders,
        "nulls_allowed": nulls_allowed,
        "consistency_review": consistency_review,
        "source_overview": source_overview,
        "priority_matrix": priority_matrix,
        "recent_runs": recent_runs,
        "narrative": narrative,
    }


def build_tables(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    return {
        **meta,
        "filters_available": filters,
        "top_customers": query_rows(
            conn,
            """
            SELECT
                COALESCE(p.razon_social, 'Sin cliente') AS cliente_nombre,
                COUNT(*)::bigint AS documentos,
                COALESCE(SUM(d.total),0)::numeric(18,2) AS monto_total
            FROM core.documentos d
            LEFT JOIN core.personas p ON p.id = d.cliente_id
            WHERE d.cliente_id IS NOT NULL
            GROUP BY 1
            ORDER BY monto_total DESC
            LIMIT 50
            """
        ),
        "top_products": query_rows(
            conn,
            """
            SELECT
                COALESCE(dd.producto_nombre, p.nombre, 'Sin producto') AS producto_nombre,
                COALESCE(c.nombre, 'Sin categoría') AS categoria_nombre,
                COALESCE(m.nombre, 'Sin marca') AS marca_nombre,
                COALESCE(SUM(dd.cantidad),0)::numeric(18,2) AS cantidad_total,
                COALESCE(SUM(dd.cantidad * dd.precio),0)::numeric(18,2) AS importe_total
            FROM core.documento_detalles dd
            LEFT JOIN core.productos p ON p.id = dd.producto_id
            LEFT JOIN core.categorias c ON c.id = p.categoria_id
            LEFT JOIN core.marcas m ON m.id = p.marca_id
            GROUP BY 1, 2, 3
            ORDER BY importe_total DESC
            LIMIT 50
            """
        ),
        "top_bodegas": query_rows(
            conn,
            """
            SELECT
                COALESCE(b.nombre, 'Sin bodega') AS bodega_nombre,
                COUNT(*)::bigint AS movimientos,
                COALESCE(SUM(m.total),0)::numeric(18,2) AS valor_total
            FROM core.movimientos m
            LEFT JOIN core.bodegas b ON b.id = m.bodega_id
            GROUP BY 1
            ORDER BY movimientos DESC, valor_total DESC
            LIMIT 50
            """
        ),
        "top_accounts": query_rows(
            conn,
            """
            SELECT
                COALESCE(cc.nombre, 'Sin cuenta') AS cuenta_nombre,
                COUNT(*)::bigint AS lineas,
                COALESCE(SUM(ad.valor),0)::numeric(18,2) AS valor_total
            FROM core.asiento_detalles ad
            LEFT JOIN core.cuentas_contables cc ON cc.id = ad.cuenta_id
            GROUP BY 1
            ORDER BY valor_total DESC
            LIMIT 50
            """
        ),
    }


def build_database(
    conn,
    meta: dict[str, Any],
    filters: dict[str, Any],
    db_name: str,
    exported_payloads: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    table_catalog = query_rows(
        conn,
        """
        SELECT
            t.table_schema,
            t.table_name,
            COALESCE(col.column_count, 0)::integer AS column_count,
            COALESCE(col.nullable_columns, 0)::integer AS nullable_columns,
            COALESCE(pk.pk_columns, '') AS pk_columns
        FROM information_schema.tables t
        LEFT JOIN (
            SELECT
                table_schema,
                table_name,
                COUNT(*) AS column_count,
                SUM(CASE WHEN is_nullable = 'YES' THEN 1 ELSE 0 END) AS nullable_columns
            FROM information_schema.columns
            WHERE table_schema IN ('meta', 'raw', 'core', 'reporting')
            GROUP BY table_schema, table_name
        ) col
            ON col.table_schema = t.table_schema
           AND col.table_name = t.table_name
        LEFT JOIN (
            SELECT
                tc.table_schema,
                tc.table_name,
                string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS pk_columns
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema IN ('meta', 'raw', 'core', 'reporting')
            GROUP BY tc.table_schema, tc.table_name
        ) pk
            ON pk.table_schema = t.table_schema
           AND pk.table_name = t.table_name
        WHERE t.table_schema IN ('meta', 'raw', 'core', 'reporting')
          AND t.table_type = 'BASE TABLE'
        ORDER BY t.table_schema, t.table_name
        """,
    )
    view_inventory = query_rows(
        conn,
        """
        SELECT table_schema AS schema_name, table_name AS view_name
        FROM information_schema.views
        WHERE table_schema IN ('meta', 'raw', 'core', 'reporting')
        ORDER BY table_schema, table_name
        """,
    )
    schema_names = ["meta", "raw", "core", "reporting"]
    schema_rollup = {
        schema_name: {
            "schema_name": schema_name,
            "table_count": 0,
            "view_count": 0,
            "total_rows": 0,
            "total_size_bytes": 0,
        }
        for schema_name in schema_names
    }
    for view in view_inventory:
        schema_rollup[view["schema_name"]]["view_count"] += 1

    table_inventory: list[dict[str, Any]] = []
    table_row_map: dict[str, int] = {}
    with conn.cursor() as cur:
        for table in table_catalog:
            schema_name = table["table_schema"]
            table_name = table["table_name"]
            qualified_name = f"{schema_name}.{table_name}"
            cur.execute(
                sql.SQL("SELECT COUNT(*)::bigint FROM {}.{}").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_name),
                )
            )
            row_count = int(cur.fetchone()[0] or 0)
            cur.execute(
                """
                SELECT
                    COALESCE(pg_total_relation_size(to_regclass(%s)), 0)::bigint AS total_size_bytes,
                    COALESCE(pg_relation_size(to_regclass(%s)), 0)::bigint AS data_size_bytes,
                    COALESCE(pg_indexes_size(to_regclass(%s)), 0)::bigint AS index_size_bytes
                """,
                (qualified_name, qualified_name, qualified_name),
            )
            size_row = cur.fetchone()
            total_size_bytes = int(size_row[0] or 0)
            data_size_bytes = int(size_row[1] or 0)
            index_size_bytes = int(size_row[2] or 0)
            row = {
                "schema_name": schema_name,
                "table_name": table_name,
                "qualified_name": qualified_name,
                "row_count": row_count,
                "column_count": int(table["column_count"] or 0),
                "nullable_columns": int(table["nullable_columns"] or 0),
                "pk_columns": table["pk_columns"],
                "total_size_bytes": total_size_bytes,
                "data_size_bytes": data_size_bytes,
                "index_size_bytes": index_size_bytes,
            }
            table_inventory.append(row)
            table_row_map[qualified_name] = row_count
            schema_rollup[schema_name]["table_count"] += 1
            schema_rollup[schema_name]["total_rows"] += row_count
            schema_rollup[schema_name]["total_size_bytes"] += total_size_bytes

    fk_catalog = query_rows(
        conn,
        """
        WITH fk_catalog AS (
            SELECT
                con.conname,
                src_ns.nspname AS source_schema,
                src.relname AS source_table,
                dst_ns.nspname AS target_schema,
                dst.relname AS target_table,
                con.condeferrable,
                con.condeferred,
                array_agg(src_att.attname ORDER BY ord.ordinality) AS source_columns,
                array_agg(dst_att.attname ORDER BY ord.ordinality) AS target_columns,
                bool_or(NOT src_att.attnotnull) AS has_nullable_child,
                COUNT(*)::integer AS column_count
            FROM pg_constraint con
            JOIN pg_class src ON src.oid = con.conrelid
            JOIN pg_namespace src_ns ON src_ns.oid = src.relnamespace
            JOIN pg_class dst ON dst.oid = con.confrelid
            JOIN pg_namespace dst_ns ON dst_ns.oid = dst.relnamespace
            JOIN unnest(con.conkey, con.confkey) WITH ORDINALITY AS ord(src_attnum, dst_attnum, ordinality) ON TRUE
            JOIN pg_attribute src_att
              ON src_att.attrelid = src.oid
             AND src_att.attnum = ord.src_attnum
            JOIN pg_attribute dst_att
              ON dst_att.attrelid = dst.oid
             AND dst_att.attnum = ord.dst_attnum
            WHERE con.contype = 'f'
              AND src_ns.nspname IN ('meta', 'raw', 'core')
              AND dst_ns.nspname IN ('meta', 'raw', 'core')
            GROUP BY
                con.oid,
                con.conname,
                src_ns.nspname,
                src.relname,
                dst_ns.nspname,
                dst.relname,
                con.condeferrable,
                con.condeferred
        )
        SELECT *
        FROM fk_catalog
        ORDER BY source_schema, source_table, conname
        """,
    )

    relationships: list[dict[str, Any]] = []
    relationship_types: dict[str, int] = {}
    density_map: dict[str, dict[str, Any]] = {}
    with conn.cursor() as cur:
        for fk in fk_catalog:
            source_columns = fk["source_columns"] or []
            target_columns = fk["target_columns"] or []
            if fk["source_schema"] == fk["target_schema"] and fk["source_table"] == fk["target_table"]:
                relation_type = "Autorrelacion 1:N"
            elif int(fk["column_count"] or 0) > 1:
                relation_type = "Relacion compuesta 1:N"
            elif fk["has_nullable_child"]:
                relation_type = "1:0..N"
            else:
                relation_type = "1:N"

            join_clauses = [
                sql.SQL("dst.{} = src.{}").format(sql.Identifier(dst_col), sql.Identifier(src_col))
                for src_col, dst_col in zip(source_columns, target_columns)
            ]
            not_null_clauses = [sql.SQL("src.{} IS NOT NULL").format(sql.Identifier(src_col)) for src_col in source_columns]
            orphan_query = sql.SQL(
                """
                SELECT COUNT(*)::bigint
                FROM {}.{} src
                LEFT JOIN {}.{} dst
                  ON {}
                WHERE {}
                  AND dst.{} IS NULL
                """
            ).format(
                sql.Identifier(fk["source_schema"]),
                sql.Identifier(fk["source_table"]),
                sql.Identifier(fk["target_schema"]),
                sql.Identifier(fk["target_table"]),
                sql.SQL(" AND ").join(join_clauses),
                sql.SQL(" AND ").join(not_null_clauses),
                sql.Identifier(target_columns[0]),
            )
            cur.execute(orphan_query)
            orphan_count = int(cur.fetchone()[0] or 0)

            source_name = f"{fk['source_schema']}.{fk['source_table']}"
            target_name = f"{fk['target_schema']}.{fk['target_table']}"
            relationships.append(
                {
                    "constraint_name": fk["conname"],
                    "source_table": source_name,
                    "source_columns": ", ".join(source_columns),
                    "target_table": target_name,
                    "target_columns": ", ".join(target_columns),
                    "relation_type": relation_type,
                    "nullable_child": bool(fk["has_nullable_child"]),
                    "deferrable": bool(fk["condeferrable"]),
                    "initially_deferred": bool(fk["condeferred"]),
                    "orphan_count": orphan_count,
                }
            )
            relationship_types[relation_type] = relationship_types.get(relation_type, 0) + 1

            source_density = density_map.setdefault(
                source_name,
                {
                    "table_name": source_name,
                    "outgoing_fks": 0,
                    "incoming_fks": 0,
                    "row_count": table_row_map.get(source_name, 0),
                },
            )
            source_density["outgoing_fks"] += 1

            target_density = density_map.setdefault(
                target_name,
                {
                    "table_name": target_name,
                    "outgoing_fks": 0,
                    "incoming_fks": 0,
                    "row_count": table_row_map.get(target_name, 0),
                },
            )
            target_density["incoming_fks"] += 1

    relationship_density = sorted(
        density_map.values(),
        key=lambda row: (row["incoming_fks"] + row["outgoing_fks"], row["row_count"]),
        reverse=True,
    )
    relationship_type_rows = [
        {"relation_type": relation_type, "relation_count": relation_count}
        for relation_type, relation_count in sorted(relationship_types.items(), key=lambda item: (-item[1], item[0]))
    ]

    column_types = query_rows(
        conn,
        """
        SELECT
            table_schema AS schema_name,
            data_type,
            COUNT(*)::bigint AS column_count
        FROM information_schema.columns
        WHERE table_schema IN ('meta', 'raw', 'core', 'reporting')
        GROUP BY table_schema, data_type
        ORDER BY column_count DESC, table_schema, data_type
        """
    )

    frontend_assets: list[dict[str, Any]] = []
    frontend_total_bytes = 0
    frontend_total_rows = 0
    for filename, payload in exported_payloads.items():
        collections = [
            {"collection_name": key, "row_count": len(value)}
            for key, value in payload.items()
            if isinstance(value, list)
        ]
        rows_exposed = sum(item["row_count"] for item in collections)
        size_bytes = payload_size_bytes(payload)
        largest_collection = max(collections, key=lambda row: row["row_count"], default=None)
        frontend_assets.append(
            {
                "file_name": filename,
                "rows_exposed": rows_exposed,
                "size_bytes": size_bytes,
                "collection_count": len(collections),
                "largest_collection": largest_collection["collection_name"] if largest_collection else None,
                "largest_collection_rows": largest_collection["row_count"] if largest_collection else 0,
            }
        )
        frontend_total_bytes += size_bytes
        frontend_total_rows += rows_exposed
    frontend_assets.sort(key=lambda row: row["size_bytes"], reverse=True)

    front_back_inventory_rules = [
        ("Comercial", ["core.documentos", "core.documento_detalles", "core.documento_cobros"], "commercial.json"),
        ("Clientes", ["core.personas"], "customers.json"),
        ("Productos", ["core.productos"], "products.json"),
        ("Inventario", ["core.movimientos", "core.movimiento_detalles"], "inventory.json"),
        ("Contabilidad", ["core.asientos", "core.asiento_detalles"], "accounting.json"),
        ("Calidad", ["meta.extract_runs", "meta.watermarks", "meta.load_metrics"], "quality.json"),
        ("Tecnica", ["meta.extract_runs", "meta.watermarks", "meta.load_metrics"], "technical.json"),
        ("Tablas analiticas", ["core.documentos", "core.productos", "core.movimientos", "core.asientos"], "tables.json"),
    ]
    frontend_asset_map = {row["file_name"]: row for row in frontend_assets}
    front_back_inventory = []
    for domain, backend_tables, file_name in front_back_inventory_rules:
        backend_rows = sum(table_row_map.get(table_name, 0) for table_name in backend_tables)
        frontend_rows = int(frontend_asset_map.get(file_name, {}).get("rows_exposed", 0))
        front_back_inventory.append(
            {
                "domain": domain,
                "backend_rows": backend_rows,
                "frontend_rows": frontend_rows,
                "frontend_file": file_name,
                "backend_scope": ", ".join(table_name.replace("core.", "").replace("meta.", "") for table_name in backend_tables),
            }
        )

    performance_rows = query_rows(
        conn,
        """
        WITH successful_runs AS (
            SELECT run_id
            FROM meta.extract_runs
            GROUP BY run_id
            HAVING SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) = 0
               AND SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) > 0
        ),
        latest_runs AS (
            SELECT
                er.run_id,
                MAX(er.finished_at) AS finished_at
            FROM meta.extract_runs er
            JOIN successful_runs sr ON sr.run_id = er.run_id
            GROUP BY er.run_id
            ORDER BY finished_at DESC
            LIMIT 5
        ),
        core_load AS (
            SELECT
                run_id,
                resource,
                SUM(row_count)::bigint AS core_rows_loaded
            FROM meta.load_metrics
            WHERE stage = 'core'
            GROUP BY run_id, resource
        )
        SELECT
            er.run_id,
            er.resource,
            er.pages_fetched,
            er.source_count,
            COALESCE(cl.core_rows_loaded, 0)::bigint AS core_rows_loaded,
            COALESCE(EXTRACT(EPOCH FROM (er.finished_at - er.started_at))::bigint, 0) AS duration_seconds
        FROM meta.extract_runs er
        JOIN latest_runs lr ON lr.run_id = er.run_id
        LEFT JOIN core_load cl
          ON cl.run_id = er.run_id
         AND cl.resource = er.resource
        WHERE er.status = 'success'
        ORDER BY er.run_id DESC, er.resource
        """
    )

    def performance_reason(row: dict[str, Any]) -> str:
        pages = int(row.get("latest_pages_fetched") or 0)
        fanout = float(row.get("fanout_ratio") or 0)
        source_rows = int(row.get("latest_source_count") or 0)
        if pages >= 200 and fanout >= 4:
            return "Paginacion alta desde API y expansion fuerte a tablas hijas durante la normalizacion."
        if pages >= 500:
            return "La mayor presion esta en I/O remoto: demasiadas paginas para recorrer en la API."
        if fanout >= 4:
            return "Cada entidad base genera multiples filas hijas, lo que amplifica el costo de carga."
        if source_rows >= 20000:
            return "El volumen base es alto incluso antes de expandir detalles o cobros."
        return "Carga controlada; el tiempo responde mas al volumen normal del recurso que a una anomalia estructural."

    def performance_hint(row: dict[str, Any]) -> str:
        pages = int(row.get("latest_pages_fetched") or 0)
        fanout = float(row.get("fanout_ratio") or 0)
        if pages >= 200:
            return "Priorizar estrategia incremental por watermark y fetch concurrente controlado por pagina."
        if fanout >= 4:
            return "Separar cabeceras y detalles en lotes dedicados y monitorear fanout esperado por recurso."
        return "Mantener monitoreo de throughput y alertar desviaciones sobre el promedio historico."

    run_groups: dict[str, list[dict[str, Any]]] = {}
    for row in performance_rows:
        run_groups.setdefault(row["run_id"], []).append(row)
    latest_performance_run_id = next(iter(run_groups.keys()), None)
    latest_run_rows = run_groups.get(latest_performance_run_id, [])
    latest_total_duration = sum(int(row.get("duration_seconds") or 0) for row in latest_run_rows)
    latest_total_pages = sum(int(row.get("pages_fetched") or 0) for row in latest_run_rows)
    latest_total_source_rows = sum(int(row.get("source_count") or 0) for row in latest_run_rows)
    latest_total_core_rows = sum(int(row.get("core_rows_loaded") or 0) for row in latest_run_rows)

    resource_history: dict[str, list[dict[str, Any]]] = {}
    for row in performance_rows:
        resource_history.setdefault(row["resource"], []).append(row)

    performance_resources = []
    for resource, rows in resource_history.items():
        latest_row = next((row for row in rows if row["run_id"] == latest_performance_run_id), rows[0])
        duration_values = [int(row.get("duration_seconds") or 0) for row in rows]
        latest_duration = int(latest_row.get("duration_seconds") or 0)
        latest_source_count = int(latest_row.get("source_count") or 0)
        latest_core_rows_loaded = int(latest_row.get("core_rows_loaded") or 0)
        latest_pages_fetched = int(latest_row.get("pages_fetched") or 0)
        source_rows_per_second = round(latest_source_count / latest_duration, 2) if latest_duration else 0
        core_rows_per_second = round(latest_core_rows_loaded / latest_duration, 2) if latest_duration else 0
        fanout_ratio = round(latest_core_rows_loaded / latest_source_count, 2) if latest_source_count else 0
        duration_share_pct = round((latest_duration / latest_total_duration) * 100, 2) if latest_total_duration else 0
        row = {
            "resource": resource,
            "latest_run_id": latest_performance_run_id,
            "latest_duration_seconds": latest_duration,
            "avg_duration_seconds": round(sum(duration_values) / len(duration_values), 2),
            "max_duration_seconds": max(duration_values),
            "latest_pages_fetched": latest_pages_fetched,
            "latest_source_count": latest_source_count,
            "latest_core_rows_loaded": latest_core_rows_loaded,
            "source_rows_per_second": source_rows_per_second,
            "core_rows_per_second": core_rows_per_second,
            "fanout_ratio": fanout_ratio,
            "duration_share_pct": duration_share_pct,
        }
        row["reason"] = performance_reason(row)
        row["optimization_hint"] = performance_hint(row)
        performance_resources.append(row)
    performance_resources.sort(key=lambda row: row["latest_duration_seconds"], reverse=True)

    performance_runs = []
    for run_id, rows in sorted(run_groups.items(), key=lambda item: item[0], reverse=True):
        total_duration = sum(int(row.get("duration_seconds") or 0) for row in rows)
        slowest = max(rows, key=lambda row: int(row.get("duration_seconds") or 0), default=None)
        performance_runs.append(
            {
                "run_id": run_id,
                "resources_processed": len(rows),
                "total_duration_seconds": total_duration,
                "total_pages_fetched": sum(int(row.get("pages_fetched") or 0) for row in rows),
                "total_source_rows": sum(int(row.get("source_count") or 0) for row in rows),
                "total_core_rows": sum(int(row.get("core_rows_loaded") or 0) for row in rows),
                "slowest_resource": slowest.get("resource") if slowest else None,
                "slowest_duration_seconds": int(slowest.get("duration_seconds") or 0) if slowest else 0,
            }
        )

    previous_run_summary = performance_runs[1] if len(performance_runs) > 1 else None
    previous_performance_run_id = previous_run_summary["run_id"] if previous_run_summary else None
    previous_resource_map = {
        row["resource"]: row
        for row in run_groups.get(previous_performance_run_id, [])
    }
    resource_comparison = []
    improved_resources = 0
    regressed_resources = 0
    unchanged_resources = 0
    for row in performance_resources:
        previous = previous_resource_map.get(row["resource"])
        if not previous:
            continue
        previous_duration = int(previous.get("duration_seconds") or 0)
        latest_duration = int(row["latest_duration_seconds"] or 0)
        delta_seconds = latest_duration - previous_duration
        delta_pct = round((delta_seconds / previous_duration) * 100, 2) if previous_duration else 0
        if delta_seconds < 0:
            status = "mejora"
            improved_resources += 1
        elif delta_seconds > 0:
            status = "regresion"
            regressed_resources += 1
        else:
            status = "sin_cambio"
            unchanged_resources += 1
        same_volume = (
            int(row.get("latest_pages_fetched") or 0) == int(previous.get("pages_fetched") or 0)
            and int(row.get("latest_source_count") or 0) == int(previous.get("source_count") or 0)
            and int(row.get("latest_core_rows_loaded") or 0) == int(previous.get("core_rows_loaded") or 0)
        )
        if same_volume and delta_seconds < 0:
            explanation = "Mismo volumen y menos tiempo: la mejora apunta a menor costo de roundtrips e insercion."
        elif same_volume and delta_seconds > 0:
            explanation = "Mismo volumen pero mayor tiempo: revisar variacion de red, API o presion del motor."
        elif delta_seconds < 0:
            explanation = "Mejora con cambios de volumen; el recurso sigue respondiendo mas rapido en el ultimo run."
        elif delta_seconds > 0:
            explanation = "El recurso empeoro respecto al run anterior; conviene revisar concurrencia y carga hija."
        else:
            explanation = "Sin cambio material frente al run anterior."
        resource_comparison.append(
            {
                "resource": row["resource"],
                "latest_duration_seconds": latest_duration,
                "previous_duration_seconds": previous_duration,
                "delta_seconds": delta_seconds,
                "delta_pct": delta_pct,
                "latest_pages_fetched": int(row.get("latest_pages_fetched") or 0),
                "previous_pages_fetched": int(previous.get("pages_fetched") or 0),
                "latest_source_count": int(row.get("latest_source_count") or 0),
                "previous_source_count": int(previous.get("source_count") or 0),
                "latest_core_rows_loaded": int(row.get("latest_core_rows_loaded") or 0),
                "previous_core_rows_loaded": int(previous.get("core_rows_loaded") or 0),
                "status": status,
                "same_volume": same_volume,
                "explanation": explanation,
            }
        )
    resource_comparison.sort(key=lambda row: abs(int(row["delta_seconds"])), reverse=True)

    run_comparison = None
    comparison_story_cards: list[dict[str, Any]] = []
    if previous_run_summary:
        total_delta_seconds = int(performance_runs[0]["total_duration_seconds"] or 0) - int(previous_run_summary["total_duration_seconds"] or 0)
        total_delta_pct = round((total_delta_seconds / int(previous_run_summary["total_duration_seconds"] or 1)) * 100, 2)
        run_comparison = {
            "latest_run_id": performance_runs[0]["run_id"],
            "previous_run_id": previous_run_summary["run_id"],
            "latest_total_duration_seconds": int(performance_runs[0]["total_duration_seconds"] or 0),
            "previous_total_duration_seconds": int(previous_run_summary["total_duration_seconds"] or 0),
            "total_delta_seconds": total_delta_seconds,
            "total_delta_pct": total_delta_pct,
            "improved_resources": improved_resources,
            "regressed_resources": regressed_resources,
            "unchanged_resources": unchanged_resources,
            "latest_slowest_resource": performance_runs[0]["slowest_resource"],
            "previous_slowest_resource": previous_run_summary["slowest_resource"],
            "latest_pages_fetched": int(performance_runs[0]["total_pages_fetched"] or 0),
            "previous_pages_fetched": int(previous_run_summary["total_pages_fetched"] or 0),
            "latest_source_rows": int(performance_runs[0]["total_source_rows"] or 0),
            "previous_source_rows": int(previous_run_summary["total_source_rows"] or 0),
            "latest_core_rows": int(performance_runs[0]["total_core_rows"] or 0),
            "previous_core_rows": int(previous_run_summary["total_core_rows"] or 0),
        }
        comparison_story_cards = [
            {
                "title": "Comparativo entre corridas",
                "body": f"La corrida {performance_runs[0]['run_id']} se compara contra {previous_run_summary['run_id']}. La variacion total es de {total_delta_seconds} segundos sobre corridas exitosas completas del pipeline.",
            },
            {
                "title": "Lectura de mejora",
                "body": f"Recursos con mejora: {improved_resources}. Recursos con regresion: {regressed_resources}. Recursos sin cambio material: {unchanged_resources}.",
            },
            {
                "title": "Interpretacion tecnica",
                "body": "Cuando el volumen y la paginacion se mantienen, una baja en tiempo sugiere menos costo de roundtrips y menos trabajo repetido durante la carga. Esa lectura es inferencia basada en las metricas del pipeline.",
            },
        ]

    slowest_resource = performance_resources[0] if performance_resources else None
    highest_fanout = max(performance_resources, key=lambda row: row["fanout_ratio"], default=None)
    highest_pages = max(performance_resources, key=lambda row: row["latest_pages_fetched"], default=None)
    performance_story_cards = [
        {
            "title": "Recurso mas costoso del refresh",
            "body": f"El recurso mas lento hoy es {slowest_resource['resource'] if slowest_resource else '--'} con {slowest_resource['latest_duration_seconds'] if slowest_resource else 0} segundos. Consume {round(slowest_resource['duration_share_pct'], 2) if slowest_resource else 0}% del tiempo de extraccion y carga del ultimo run.",
        },
        {
            "title": "Principal driver tecnico",
            "body": f"El mayor recorrido por paginas esta en {highest_pages['resource'] if highest_pages else '--'} con {highest_pages['latest_pages_fetched'] if highest_pages else 0} paginas. El mayor fanout lo presenta {highest_fanout['resource'] if highest_fanout else '--'} con una expansion de {highest_fanout['fanout_ratio'] if highest_fanout else 0} filas core por fila fuente.",
        },
        {
            "title": "Lectura operativa",
            "body": f"En el ultimo run se recorrieron {latest_total_pages} paginas, se recuperaron {latest_total_source_rows} filas fuente y se materializaron {latest_total_core_rows} filas core. El punto critico no es solo el volumen, sino la combinacion entre paginacion remota y expansion a detalles.",
        },
    ]

    schema_storage = sorted(schema_rollup.values(), key=lambda row: row["total_size_bytes"], reverse=True)
    table_inventory.sort(key=lambda row: row["total_size_bytes"], reverse=True)
    total_backend_rows = sum(row["row_count"] for row in table_inventory)
    core_rows = sum(row["row_count"] for row in table_inventory if row["schema_name"] == "core")
    raw_rows = sum(row["row_count"] for row in table_inventory if row["schema_name"] == "raw")
    meta_rows = sum(row["row_count"] for row in table_inventory if row["schema_name"] == "meta")
    total_database_size = sum(row["total_size_bytes"] for row in table_inventory)
    relationship_total = len(relationships)
    optional_relationships = sum(1 for row in relationships if row["nullable_child"])
    self_relationships = sum(1 for row in relationships if row["relation_type"] == "Autorrelacion 1:N")
    composite_relationships = sum(1 for row in relationships if row["relation_type"] == "Relacion compuesta 1:N")
    total_orphans = sum(int(row["orphan_count"] or 0) for row in relationships)
    most_referenced = max(relationship_density, key=lambda row: row["incoming_fks"], default=None)
    most_dependent = max(relationship_density, key=lambda row: row["outgoing_fks"], default=None)
    largest_table = max(table_inventory, key=lambda row: row["total_size_bytes"], default=None)

    story_cards = [
        {
            "title": "Base maestra relacional",
            "body": f"La base {db_name} concentra {len(table_inventory)} tablas base, {len(view_inventory)} vistas y {relationship_total} relaciones FK materializadas. El esquema core domina el volumen con {core_rows} filas y soporta toda la capa analitica.",
        },
        {
            "title": "Enfoque en relaciones",
            "body": f"El modelo privilegia relaciones 1:N normalizadas. Hay {optional_relationships} relaciones opcionales, {self_relationships} autorrelaciones y {composite_relationships} relaciones compuestas. El conteo de huerfanos observado hoy es {total_orphans}.",
        },
        {
            "title": "Back versus front",
            "body": f"El backend conserva {total_backend_rows} filas entre meta, raw y core, mientras el snapshot web expone {frontend_total_rows} registros agregados en {len(frontend_assets)} archivos JSON para exploracion rapida sin abrir la base al navegador.",
        },
        {
            "title": "Puntos de mayor acoplamiento",
            "body": f"La tabla mas referenciada es {most_referenced['table_name'] if most_referenced else '--'} y la mas dependiente es {most_dependent['table_name'] if most_dependent else '--'}. El mayor volumen fisico hoy esta en {largest_table['qualified_name'] if largest_table else '--'}.",
        },
    ]

    return {
        **meta,
        "filters_available": filters,
        "summary": {
            "database_name": db_name,
            "schema_count": len(schema_names),
            "table_count": len(table_inventory),
            "view_count": len(view_inventory),
            "relationship_count": relationship_total,
            "backend_total_rows": total_backend_rows,
            "core_rows": core_rows,
            "raw_rows": raw_rows,
            "meta_rows": meta_rows,
            "frontend_total_rows": frontend_total_rows,
            "database_total_size_bytes": total_database_size,
            "frontend_total_size_bytes": frontend_total_bytes,
            "optional_relationships": optional_relationships,
            "total_orphans": total_orphans,
        },
        "story_cards": story_cards,
        "schema_storage": schema_storage,
        "table_inventory": table_inventory,
        "view_inventory": view_inventory,
        "relationship_types": relationship_type_rows,
        "relationships": relationships,
        "relationship_density": relationship_density,
        "column_types": column_types,
        "frontend_assets": frontend_assets,
        "front_back_inventory": front_back_inventory,
        "performance_summary": {
            "latest_run_id": latest_performance_run_id,
            "latest_total_duration_seconds": latest_total_duration,
            "latest_total_pages": latest_total_pages,
            "latest_total_source_rows": latest_total_source_rows,
            "latest_total_core_rows": latest_total_core_rows,
            "slowest_resource": slowest_resource["resource"] if slowest_resource else None,
            "slowest_duration_seconds": slowest_resource["latest_duration_seconds"] if slowest_resource else 0,
            "highest_fanout_resource": highest_fanout["resource"] if highest_fanout else None,
            "highest_fanout_ratio": highest_fanout["fanout_ratio"] if highest_fanout else 0,
            "highest_pages_resource": highest_pages["resource"] if highest_pages else None,
            "highest_pages_fetched": highest_pages["latest_pages_fetched"] if highest_pages else 0,
        },
        "performance_story_cards": performance_story_cards,
        "performance_resources": performance_resources,
        "performance_runs": performance_runs,
        "performance_comparison": run_comparison,
        "performance_comparison_story_cards": comparison_story_cards,
        "performance_resource_comparison": resource_comparison,
    }


def export_dashboard_data(args: argparse.Namespace) -> int:
    config = pg_config_from_env(args.db_name)
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    with open_connection(config, config.db_name) as conn:
        meta = base_metadata(conn)
        filters = filters_available(conn)
        payloads = {
            "manifest.json": build_manifest(conn, meta, filters),
            "overview.json": build_overview(conn, meta, filters),
            "commercial.json": build_commercial(conn, meta, filters),
            "customers.json": build_customers(conn, meta, filters),
            "products.json": build_products(conn, meta, filters),
            "inventory.json": build_inventory(conn, meta, filters),
            "accounting.json": build_accounting(conn, meta, filters),
            "quality.json": build_quality(conn, meta, filters),
            "technical.json": build_technical(conn, meta, filters),
            "tables.json": build_tables(conn, meta, filters),
        }
        payloads["database.json"] = build_database(conn, meta, filters, args.db_name, payloads)
    for filename, payload in payloads.items():
        write_json(out_dir / filename, payload)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export PostgreSQL analytics snapshot for the Contifico dashboard")
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--out-dir", default="dashboard/data")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return export_dashboard_data(args)


if __name__ == "__main__":
    raise SystemExit(main())
