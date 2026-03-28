from __future__ import annotations

import argparse
import datetime as dt
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

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
            "suggested_action": "Revisar si son anulaciones, cargas parciales o errores de integracion. Excluirlos del analisis operativo o reconstruir los detalles antes de consolidar.",
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
            "suggested_action": "Validar costo unitario, politica de ingresos a costo cero y recalcular el total del movimiento cuando corresponda.",
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
            "title": "Productos con stock negativo",
            "metric": query_value(conn, "SELECT COUNT(*)::bigint FROM core.productos WHERE cantidad_stock < 0", default=0),
            "issue": "Existen productos con saldo de stock menor que cero.",
            "impact": "Rompe consistencia de inventario, puede esconder faltantes fisicos o movimientos no registrados.",
            "suggested_action": "Revisar el kardex del producto, regularizar entradas y salidas pendientes y bloquear analisis de rotacion hasta corregir el saldo.",
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
            "suggested_action": "Completar la gobernanza del catalogo y marcar como opcional solo los productos que realmente no usan marca.",
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
            "suggested_action": "Asignar cuenta de compra por producto o heredarla desde categoria cuando aplique.",
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
            "suggested_action": "Completar la cuenta de costo a nivel de producto o definir una regla de herencia desde la categoria.",
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
            "suggested_action": "Completar el mapeo por categoria y separar categorias de servicio de categorias inventariables para no mezclar reglas.",
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
            "suggested_action": "Separarlas explicitamente como servicios o cargos directos y revisar si algunas debieron codificarse como producto.",
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
            "suggested_action": "Depurar el plan contable operativo o clasificar cuentas vigentes sin uso para no tratarlas como anomalias futuras.",
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


def build_technical(conn, meta: dict[str, Any], filters: dict[str, Any]) -> dict[str, Any]:
    latest_run_id = meta.get("run_id")
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
    last_run = recent_runs[0] if recent_runs else {}
    total_core_rows = sum(int(row.get("row_count") or 0) for row in load_metrics if row.get("stage") == "core")
    orphan_total = sum(int(row.get("orphan_count") or 0) for row in fk_health)
    movement_anomaly = next((row for row in source_vs_core if row.get("resource") == "movimiento-inventario"), None)
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
        f"La ultima corrida proceso {int(last_run.get('resources_processed') or 0)} recursos, actualizo {updated_tables} tablas y materializo {total_core_rows} filas core.",
        f"La diferencia fuente vs core mas visible sigue en movimiento-inventario con {int(movement_anomaly.get('difference') or 0) if movement_anomaly else 0} filas de brecha despues de deduplicacion.",
        f"La salud referencial consolidada registra {orphan_total} huerfanos y {sum(int(row.get('placeholder_count') or 0) for row in placeholders)} placeholders controlados.",
    ]
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
        "summary": {
            "status": last_run.get("status", "unknown"),
            "resources_processed": int(last_run.get("resources_processed") or 0),
            "resources_success": resources_success,
            "resources_failed": resources_failed,
            "tables_updated": updated_tables,
            "core_rows_updated": total_core_rows,
            "source_rows_processed": int(last_run.get("source_rows") or 0),
            "raw_rows_processed": int(last_run.get("raw_rows") or 0),
        },
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
