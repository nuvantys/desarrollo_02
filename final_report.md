# Informe Final

## Infraestructura

- Fecha de corrida: 2026-03-28
- Base PostgreSQL: `contifico_backfill` en `127.0.0.1:5432`
- Modo: `backfill`
- API base: `https://api.contifico.com`
- Raw auditado: `si`
- Run ID: `20260328180214`

## Recursos procesados

| resource | status | source_count | pages_fetched | raw_row_count | started_at | finished_at |
| --- | --- | --- | --- | --- | --- | --- |
| bodega | success | 21 | 1 | 21 | 2026-03-28 13:02:14-05:00 | 2026-03-28 13:02:15-05:00 |
| categoria | success | 22 | 1 | 22 | 2026-03-28 13:02:14-05:00 | 2026-03-28 13:02:14-05:00 |
| cuenta-contable | success | 572 | 1 | 572 | 2026-03-28 13:02:14-05:00 | 2026-03-28 13:02:14-05:00 |
| centro-costo | success | 2 | 1 | 2 | 2026-03-28 13:02:15-05:00 | 2026-03-28 13:02:15-05:00 |
| contabilidad/periodo | success | 8 | 1 | 8 | 2026-03-28 13:02:15-05:00 | 2026-03-28 13:02:15-05:00 |
| marca | success | 16 | 1 | 16 | 2026-03-28 13:02:15-05:00 | 2026-03-28 13:02:15-05:00 |
| persona | success | 2060 | 21 | 2060 | 2026-03-28 13:02:15-05:00 | 2026-03-28 13:02:22-05:00 |
| unidad | success | 20 | 1 | 20 | 2026-03-28 13:02:15-05:00 | 2026-03-28 13:02:15-05:00 |
| producto | success | 1237 | 13 | 1237 | 2026-03-28 13:02:22-05:00 | 2026-03-28 13:02:40-05:00 |
| movimiento-inventario | success | 14101 | 142 | 46146 | 2026-03-28 13:02:40-05:00 | 2026-03-28 13:04:21-05:00 |
| documento | success | 23911 | 240 | 125397 | 2026-03-28 13:04:21-05:00 | 2026-03-28 13:13:12-05:00 |
| documento/tickets | success | 23911 | 240 | 101358 | 2026-03-28 13:13:13-05:00 | 2026-03-28 13:29:54-05:00 |
| contabilidad/asiento | success | 67402 | 675 | 346756 | 2026-03-28 13:29:54-05:00 | 2026-03-28 13:35:14-05:00 |

## Filas cargadas por tabla

| resource | stage | table_name | row_count |
| --- | --- | --- | --- |
| contabilidad/asiento | core | asiento_detalles | 279354 |
| contabilidad/asiento | core | asientos | 67402 |
| bodega | core | bodegas | 21 |
| categoria | core | categorias | 22 |
| centro-costo | core | centros_costo | 2 |
| cuenta-contable | core | cuentas_contables | 572 |
| documento | core | documento_cobros | 24039 |
| documento | core | documento_detalles | 77447 |
| documento | core | documentos | 23911 |
| marca | core | marcas | 16 |
| movimiento-inventario | core | movimiento_detalles | 37122 |
| movimiento-inventario | core | movimientos | 9024 |
| contabilidad/periodo | core | periodos | 8 |
| persona | core | personas | 2060 |
| producto | core | productos | 1237 |
| documento/tickets | core | tickets_detalles | 77447 |
| documento/tickets | core | tickets_documentos | 23911 |
| unidad | core | unidades | 20 |
| contabilidad/asiento | raw | resource_rows | 346756 |
| cuenta-contable | raw | resource_rows | 572 |
| categoria | raw | resource_rows | 22 |
| bodega | raw | resource_rows | 21 |
| marca | raw | resource_rows | 16 |
| unidad | raw | resource_rows | 20 |
| centro-costo | raw | resource_rows | 2 |
| contabilidad/periodo | raw | resource_rows | 8 |
| persona | raw | resource_rows | 2060 |
| producto | raw | resource_rows | 1237 |
| movimiento-inventario | raw | resource_rows | 46146 |
| documento | raw | resource_rows | 125397 |
| documento/tickets | raw | resource_rows | 101358 |

## Fuente vs tabla principal

| resource | source_count | core_primary_count | difference |
| --- | --- | --- | --- |
| bodega | 21 | 21 | 0 |
| categoria | 22 | 23 | -1 |
| cuenta-contable | 572 | 572 | 0 |
| centro-costo | 2 | 2 | 0 |
| contabilidad/periodo | 8 | 8 | 0 |
| marca | 16 | 16 | 0 |
| persona | 2060 | 2060 | 0 |
| unidad | 20 | 20 | 0 |
| producto | 1237 | 1237 | 0 |
| movimiento-inventario | 14101 | 9024 | 5077 |
| documento | 23911 | 23911 | 0 |
| documento/tickets | 23911 | 23911 | 0 |
| contabilidad/asiento | 67402 | 67402 | 0 |

## Cobertura temporal

| resource | row_count | min_date | max_date |
| --- | --- | --- | --- |
| asientos | 67402 | 2019-03-12 | 2026-07-08 |
| documentos | 23911 | 2019-03-12 | 2026-03-28 |
| movimientos | 9024 | 2022-05-16 | 2026-03-28 |
| periodos | 8 | 2019-01-01 | 2026-12-31 |
| personas | 2060 | 2019-11-20 | 2026-03-27 |
| productos | 1237 | 2019-11-14 | 2026-03-25 |
| tickets_documentos | 23911 | 2019-03-12 | 2026-03-28 |

## Salud relacional

| relation_name | orphan_count |
| --- | --- |
| asiento_detalles.asiento_id -> asientos.id | 0 |
| asiento_detalles.centro_costo_id -> centros_costo.id | 0 |
| asiento_detalles.cuenta_id -> cuentas_contables.id | 0 |
| categorias.cuenta_compra -> cuentas_contables.id | 0 |
| categorias.cuenta_inventario -> cuentas_contables.id | 0 |
| categorias.cuenta_venta -> cuentas_contables.id | 0 |
| categorias.padre_id -> categorias.id | 0 |
| centros_costo.padre_id -> centros_costo.id | 0 |
| documento_cobros.documento_id -> documentos.id | 0 |
| documento_detalles.centro_costo_id -> centros_costo.id | 0 |
| documento_detalles.cuenta_id -> cuentas_contables.id | 0 |
| documento_detalles.documento_id -> documentos.id | 0 |
| documento_detalles.producto_id -> productos.id | 0 |
| documentos.cliente_id -> personas.id | 0 |
| documentos.documento_relacionado_id -> documentos.id | 0 |
| documentos.persona_id -> personas.id | 0 |
| documentos.proveedor_id -> personas.id | 0 |
| documentos.vendedor_id -> personas.id | 0 |
| movimiento_detalles.movimiento_id -> movimientos.id | 0 |
| movimiento_detalles.producto_id -> productos.id | 0 |
| movimiento_detalles.unidad_id -> unidades.id | 0 |
| movimientos.bodega_destino_id -> bodegas.id | 0 |
| movimientos.bodega_id -> bodegas.id | 0 |
| movimientos.cuenta_id -> cuentas_contables.id | 0 |
| personas.categoria_id -> categorias.id | 0 |
| personas.cuenta_por_cobrar_id -> cuentas_contables.id | 0 |
| personas.cuenta_por_pagar_id -> cuentas_contables.id | 0 |
| personas.personaasociada_id -> personas.id | 0 |
| personas.vendedor_asignado_id -> personas.id | 0 |
| productos.categoria_id -> categorias.id | 0 |
| productos.cuenta_compra_id -> cuentas_contables.id | 0 |
| productos.cuenta_costo_id -> cuentas_contables.id | 0 |
| productos.cuenta_venta_id -> cuentas_contables.id | 0 |
| productos.marca_id -> marcas.id | 0 |
| productos.producto_base_id -> productos.id | 0 |
| productos.unidad_id -> unidades.id | 0 |
| tickets_detalles.centro_costo_id -> centros_costo.id | 0 |
| tickets_detalles.documento_id -> tickets_documentos.id | 0 |
| tickets_detalles.producto_id -> productos.id | 0 |
| tickets_documentos.id -> documentos.id | 0 |

## Nulos y observaciones críticas

| indicador | total |
| --- | --- |
| tickets_items | 0 |
| movimiento_detalles_producto_id_null | 0 |
| documentos_sin_persona_id | 0 |
| tickets_detalles_producto_id_null_permitido | 17555 |
| documento_detalles_producto_id_null_permitido | 17555 |

## Placeholders relacionales

| table_name | placeholder_count |
| --- | --- |
| categorias | 1 |
| cuentas_contables | 0 |
| personas | 0 |
| productos | 0 |
| documentos | 0 |
| bodegas | 0 |
| unidades | 0 |
| marcas | 0 |
| centros_costo | 0 |

## Resumen de negocio

### Personas por rol

| rol | total |
| --- | --- |
| cliente | 1426 |
| empleado | 14 |
| proveedor | 710 |
| vendedor | 11 |

### Productos por estado y categoría

| estado | categoria | total_productos | stock_total |
| --- | --- | --- | --- |
| A | REPUESTOS VARIOS | 632 | 15445.590000 |
| A | REPUESTOS | 294 | 13837.090000 |
| A | PEZONERAS | 38 | 1798.000000 |
| I | REPUESTOS VARIOS | 35 | 7.000000 |
| A | MANGUERAS | 33 | 7795.790000 |
| A | CONSUMIBLES | 26 | 1565.950000 |
| A | BOMBAS DE VACIO | 25 | 153.000000 |
| I | REPUESTOS | 20 | 6.000000 |
| A | MOTORES A GASOLINA | 14 | 12.000000 |
| A | MANO DE OBRA | 12 | 0.000000 |
| A | TANQUES DE ENFRIAMIENTO | 12 | 12.000000 |
| I | TANQUES DE ENFRIAMIENTO | 12 | 0.000000 |
| A | PANELES SOLARES | 11 | 0.000000 |
| A | MOTORES ELECTRICOS | 9 | 5.000000 |
| I | BOMBAS DE VACIO | 9 | 0.000000 |

### Movimientos por tipo y bodega

| tipo | bodega | total_movimientos | valor_total |
| --- | --- | --- | --- |
| EGR | BODEGA PRINCIPAL | 3465 | 1636971.570000 |
| EGR | ALMACEN MACHACHI | 2126 | 125003.220000 |
| ING | BODEGA PRINCIPAL | 963 | 1913894.130000 |
| TRA | BODEGA PRINCIPAL | 839 | 305444.670000 |
| EGR | BODEGA LUIS CONDOR | 816 | 36945.440000 |
| TRA | ALMACEN MACHACHI | 225 | 26712.860000 |
| ING | ALMACEN MACHACHI | 105 | 10380.030000 |
| TRA | BODEGA LUIS CONDOR | 71 | 4377.550000 |
| EGR | CESAR CUAMACAS | 58 | 34362.400000 |
| ING | BODEGA LUIS CONDOR | 43 | 1630.850000 |
| EGR | JUAN CARLOS INSUASTI | 35 | 4430.520000 |
| EGR | JOSELYN MEJIA | 33 | 32916.950000 |
| EGR | MARGIT CEVALLOS | 32 | 961.720000 |
| TRA | CESAR CUAMACAS | 22 | 7050.890000 |
| TRA | MIGUEL MEJIA | 17 | 5359.950000 |

### Documentos por tipo y estado

| tipo_documento | estado | total_documentos | monto_total |
| --- | --- | --- | --- |
| FAC | C | 10122 | 5729567.010000 |
| FAC | G | 8776 | 2421949.270000 |
| COT | E | 1124 | 1046731.900000 |
| COT | P | 845 | 1953244.620000 |
| DAC | G | 480 | 1724818.990000 |
| DAC | C | 477 | 842444.370000 |
| FAC | P | 383 | 337512.770000 |
| NCT | G | 374 | 272446.350000 |
| NAI | G | 349 | 228919.720000 |
| DNA | G | 345 | 259188.070000 |
| NVE | G | 227 | 52024.210000 |
| CVE | G | 160 | 1362049.750000 |
| FAC | A | 61 | 70231.810000 |
| LQC | G | 50 | 28565.620000 |
| NCT | C | 50 | 18430.220000 |

### Asientos por fecha

| fecha | total_asientos |
| --- | --- |
| 2026-07-08 | 1 |
| 2026-06-25 | 1 |
| 2026-06-18 | 1 |
| 2026-06-08 | 1 |
| 2026-05-25 | 1 |
| 2026-05-18 | 1 |
| 2026-05-08 | 2 |
| 2026-04-25 | 2 |
| 2026-04-23 | 1 |
| 2026-04-20 | 2 |
| 2026-04-18 | 1 |
| 2026-04-15 | 2 |
| 2026-04-11 | 1 |
| 2026-04-10 | 1 |
| 2026-04-08 | 1 |

## Incidencias

- El backfill histórico usa paginación exhaustiva completa y no depende de filtros de fecha del backend.
- El endpoint `movimiento-inventario` reportó un `count` mayor al número final de IDs únicos materializados; se conservó la versión única de cada movimiento.
- `documento_detalles.producto_id` y `tickets_detalles.producto_id` aceptan `null`; esos registros se conservaron sin romper integridad.
- Los campos sin catálogo validado quedaron como atributos simples: `caja_id`, `cuenta_bancaria_id`, `banco_codigo_id`, `tarjeta_consumo_id`, `logistica`, `orden_domicilio_id`, `proyecto`.
- Cuando una referencia no vino en el catálogo origen, se creó un placeholder controlado para mantener la FK y dejar trazabilidad de la anomalía.
- `tickets_items` se cargó únicamente cuando el payload incluyó elementos en `tickets[]`; en ausencia de items, el detalle igualmente quedó preservado.
