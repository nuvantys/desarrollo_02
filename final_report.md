# Informe Final

## Infraestructura

- Fecha de corrida: 2026-04-02
- Base PostgreSQL destino: `postgres` en `aws-1-us-east-1.pooler.supabase.com:5432`
- Modo: `refresh`
- API base: `https://api.contifico.com`
- Raw auditado: `no`
- Run ID: `20260402191655`

## Recursos procesados

| resource | status | source_count | pages_fetched | raw_row_count | started_at | finished_at |
| --- | --- | --- | --- | --- | --- | --- |
| categoria | success | 22 | 1 | 0 | 2026-04-02 19:16:55+00:00 | 2026-04-02 19:16:55+00:00 |
| cuenta-contable | success | 572 | 1 | 0 | 2026-04-02 19:16:55+00:00 | 2026-04-02 19:16:55+00:00 |
| bodega | success | 21 | 1 | 0 | 2026-04-02 19:16:56+00:00 | 2026-04-02 19:16:56+00:00 |
| centro-costo | success | 2 | 1 | 0 | 2026-04-02 19:16:56+00:00 | 2026-04-02 19:16:57+00:00 |
| marca | success | 16 | 1 | 0 | 2026-04-02 19:16:56+00:00 | 2026-04-02 19:16:56+00:00 |
| unidad | success | 20 | 1 | 0 | 2026-04-02 19:16:56+00:00 | 2026-04-02 19:16:56+00:00 |
| banco/cuenta | success | 5 | 1 | 0 | 2026-04-02 19:16:57+00:00 | 2026-04-02 19:16:57+00:00 |
| contabilidad/periodo | success | 8 | 1 | 0 | 2026-04-02 19:16:57+00:00 | 2026-04-02 19:16:57+00:00 |
| persona | success | 2063 | 21 | 0 | 2026-04-02 19:16:57+00:00 | 2026-04-02 19:16:59+00:00 |
| producto | success | 1 | 2 | 0 | 2026-04-02 19:16:59+00:00 | 2026-04-02 19:17:08+00:00 |
| movimiento-inventario | success | 44 | 2 | 0 | 2026-04-02 19:17:08+00:00 | 2026-04-02 19:17:13+00:00 |
| documento | success | 161 | 171 | 0 | 2026-04-02 19:17:13+00:00 | 2026-04-02 19:18:07+00:00 |
| inventario/guia | success | 38 | 1 | 0 | 2026-04-02 19:18:07+00:00 | 2026-04-02 19:18:08+00:00 |
| banco/movimiento | success | 2762 | 1 | 0 | 2026-04-02 19:18:08+00:00 | 2026-04-02 19:18:24+00:00 |
| documento/tickets | success | 89 | 89 | 0 | 2026-04-02 19:18:24+00:00 | 2026-04-02 19:18:27+00:00 |
| contabilidad/asiento | success | 117 | 2 | 0 | 2026-04-02 19:18:28+00:00 | 2026-04-02 19:18:29+00:00 |

## Filas cargadas por tabla

| resource | stage | table_name | row_count |
| --- | --- | --- | --- |
| contabilidad/asiento | core | asiento_detalles | 475 |
| contabilidad/asiento | core | asientos | 117 |
| banco/cuenta | core | banco_cuentas | 5 |
| banco/movimiento | core | banco_movimiento_detalles | 3224 |
| banco/movimiento | core | banco_movimientos | 2762 |
| bodega | core | bodegas | 21 |
| categoria | core | categorias | 22 |
| centro-costo | core | centros_costo | 2 |
| cuenta-contable | core | cuentas_contables | 572 |
| documento | core | documento_cobros | 35 |
| documento | core | documento_detalles | 313 |
| documento | core | documentos | 89 |
| inventario/guia | core | guia_destinatarios | 38 |
| inventario/guia | core | guia_detalles | 72 |
| inventario/guia | core | guias | 38 |
| marca | core | marcas | 16 |
| movimiento-inventario | core | movimiento_detalles | 137 |
| movimiento-inventario | core | movimientos | 44 |
| contabilidad/periodo | core | periodos | 8 |
| persona | core | personas | 47 |
| producto | core | productos | 1 |
| documento/tickets | core | tickets_detalles | 313 |
| documento/tickets | core | tickets_documentos | 89 |
| unidad | core | unidades | 20 |

## Fuente vs tabla principal

| resource | source_count | core_primary_count | difference |
| --- | --- | --- | --- |
| categoria | 22 | 23 | -1 |
| cuenta-contable | 572 | 572 | 0 |
| bodega | 21 | 21 | 0 |
| centro-costo | 2 | 2 | 0 |
| marca | 16 | 16 | 0 |
| unidad | 20 | 20 | 0 |
| banco/cuenta | 5 | 5 | 0 |
| contabilidad/periodo | 8 | 8 | 0 |
| persona | 2063 | 2063 | 0 |
| producto | 1 | 1237 | -1236 |
| movimiento-inventario | 44 | 9057 | -9013 |
| documento | 161 | 23979 | -23818 |
| inventario/guia | 38 | 38 | 0 |
| banco/movimiento | 2762 | 2762 | 0 |
| documento/tickets | 89 | 23979 | -23890 |
| contabilidad/asiento | 117 | 67519 | -67402 |

## Cobertura temporal

| resource | row_count | min_date | max_date |
| --- | --- | --- | --- |
| asientos | 67519 | 2019-03-12 | 2026-07-08 |
| banco_cuentas | 5 | 2021-02-17 | 2025-09-03 |
| banco_movimientos | 2762 | 2019-12-20 | 2026-03-31 |
| documentos | 23979 | 2019-03-12 | 2026-04-02 |
| guias | 38 | 2020-11-26 | 2026-02-13 |
| movimientos | 9057 | 2022-05-16 | 2026-04-02 |
| periodos | 8 | 2019-01-01 | 2026-12-31 |
| personas | 2063 | 2019-11-20 | 2026-04-02 |
| productos | 1237 | 2019-11-14 | 2026-03-25 |
| tickets_documentos | 23979 | 2019-03-12 | 2026-04-02 |

## Salud relacional

| relation_name | orphan_count |
| --- | --- |
| asiento_detalles.asiento_id -> asientos.id | 0 |
| asiento_detalles.centro_costo_id -> centros_costo.id | 0 |
| asiento_detalles.cuenta_id -> cuentas_contables.id | 0 |
| banco_cuentas.cuenta_contable_id -> cuentas_contables.id | 0 |
| banco_movimiento_detalles.centro_costo_id -> centros_costo.id | 0 |
| banco_movimiento_detalles.cuenta_id -> cuentas_contables.id | 0 |
| banco_movimiento_detalles.movimiento_id -> banco_movimientos.id | 0 |
| banco_movimientos.cuenta_bancaria_id -> banco_cuentas.id | 0 |
| banco_movimientos.persona_id -> personas.id | 0 |
| categorias.cuenta_compra -> cuentas_contables.id | 0 |
| categorias.cuenta_inventario -> cuentas_contables.id | 0 |
| categorias.cuenta_venta -> cuentas_contables.id | 0 |
| categorias.padre_id -> categorias.id | 0 |
| centros_costo.padre_id -> centros_costo.id | 0 |
| documento_cobros.cuenta_bancaria_id -> banco_cuentas.id | 0 |
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
| guia_destinatarios.destinatario_id -> personas.id | 0 |
| guia_destinatarios.documento_id -> documentos.id | 0 |
| guia_destinatarios.guia_id -> guias.id | 0 |
| guia_detalles.guia_id -> guias.id | 0 |
| guia_detalles.producto_id -> productos.id | 0 |
| guias.bodega_id -> bodegas.id | 0 |
| guias.transportista_id -> personas.id | 0 |
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
| documentos_sin_persona_id | 0 |
| documento_detalles_producto_id_null_permitido | 17614 |
| tickets_detalles_producto_id_null_permitido | 17614 |
| movimiento_detalles_producto_id_null | 0 |
| tickets_items | 0 |
| guia_detalles_producto_id_null | 0 |
| banco_movimiento_detalles_cuenta_id_null | 3224 |

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
| cliente | 1429 |
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
| EGR | BODEGA PRINCIPAL | 3481 | 1638929.750000 |
| EGR | ALMACEN MACHACHI | 2134 | 125643.720000 |
| ING | BODEGA PRINCIPAL | 967 | 1914194.980000 |
| TRA | BODEGA PRINCIPAL | 839 | 305444.670000 |
| EGR | BODEGA LUIS CONDOR | 819 | 36966.130000 |
| TRA | ALMACEN MACHACHI | 226 | 26723.320000 |
| ING | ALMACEN MACHACHI | 105 | 10380.030000 |
| TRA | BODEGA LUIS CONDOR | 72 | 4379.230000 |
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
| FAC | C | 10132 | 5731434.740000 |
| FAC | G | 8793 | 2425813.150000 |
| COT | E | 1126 | 1047517.360000 |
| COT | P | 852 | 1961277.590000 |
| DAC | G | 480 | 1724818.990000 |
| DAC | C | 478 | 845674.370000 |
| FAC | P | 409 | 345771.060000 |
| NCT | G | 375 | 272477.370000 |
| NAI | G | 349 | 228919.720000 |
| DNA | G | 345 | 259188.070000 |
| NVE | G | 227 | 52024.210000 |
| CVE | G | 161 | 1363301.750000 |
| FAC | A | 61 | 70231.810000 |
| LQC | G | 51 | 28647.250000 |
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

### Guias por estado y bodega

| estado | bodega | total_guias | documentos_vinculados | cantidad_total |
| --- | --- | --- | --- | --- |
| E | BODEGA PRINCIPAL | 36 | 7 | 335.000000 |
| E | MIGUEL MEJIA | 1 | 1 | 50.000000 |
| E | SANDRO CHAMORRO | 1 | 0 | 2.000000 |

### Movimientos bancarios por tipo y cuenta

| tipo_registro | cuenta_bancaria | total_movimientos | monto_total |
| --- | --- | --- | --- |
| E | BANCO DE GUAYAQUIL | 1271 | 827603.830000 |
| E | BANCO PICHINCHA | 984 | 1834866.010000 |
| I | BANCO PICHINCHA | 355 | 512206.090000 |
| I | BANCO DE GUAYAQUIL | 93 | 290150.170000 |
| I | BANECUADOR | 29 | 29165.000000 |
| E | BANECUADOR | 25 | 15226.260000 |
| E | BANCO DINERS CLUB S.A. | 2 | 17490.970000 |
| I | BANCO DINERS CLUB S.A. | 2 | 13637.700000 |
| I | BANCO BOLIVARIANO | 1 | 700.000000 |

## Incidencias

- El backfill histórico usa paginación exhaustiva completa y no depende de filtros de fecha del backend.
- El endpoint `movimiento-inventario` reportó un `count` mayor al número final de IDs únicos materializados; se conservó la versión única de cada movimiento.
- `documento_detalles.producto_id` y `tickets_detalles.producto_id` aceptan `null`; esos registros se conservaron sin romper integridad.
- La integración nueva conecta `documento_cobros.cuenta_bancaria_id` con `core.banco_cuentas` y agrega `guias` y `banco_movimientos` como capas nuevas de logística y tesorería.
- Los campos sin catálogo validado quedaron como atributos simples: `caja_id`, `banco_codigo_id`, `tarjeta_consumo_id`, `logistica`, `orden_domicilio_id`, `proyecto`.
- Cuando una referencia no vino en el catálogo origen, se creó un placeholder controlado para mantener la FK y dejar trazabilidad de la anomalía.
- `tickets_items` se cargó únicamente cuando el payload incluyó elementos en `tickets[]`; en ausencia de items, el detalle igualmente quedó preservado.
