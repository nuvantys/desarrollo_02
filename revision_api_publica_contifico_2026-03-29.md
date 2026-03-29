# Revisión Pública de APIs Contifico

Fecha: 2026-03-29

## Objetivo

Contrastar la documentación pública de Contifico con el modelo actualmente cargado en `contifico_backfill`, validando con la credencial activa qué APIs adicionales responden hoy y qué datos nuevos se pueden incorporar.

## Alcance actual ya implementado

La base actual ya cubre el núcleo operativo y analítico:

- `personas`: 2.060
- `productos`: 1.237
- `movimientos`: 9.024
- `movimiento_detalles`: 37.122
- `documentos`: 23.911
- `documento_detalles`: 77.447
- `documento_cobros`: 24.039
- `tickets_documentos`: 23.911
- `tickets_detalles`: 77.447
- `asientos`: 67.402
- `asiento_detalles`: 279.354
- catálogos: `categorias`, `bodegas`, `marcas`, `unidades`, `cuentas_contables`, `centros_costo`, `periodos`

Esto ya permite análisis comercial, inventario, documentos, tickets y contabilidad.

## APIs públicas adicionales detectadas

La documentación pública lista, además del alcance ya explotado, estos grupos relevantes:

- `GET /sistema/api/v1/variante/`
- `GET /sistema/api/v1/inventario/guia/`
- `GET /sistema/api/v1/banco/cuenta/`
- `GET /sistema/api/v1/banco/movimiento/`
- `GET /sistema/api/v1/registro/transaccion/`
- `GET /sistema/api/v1/rrhh/rol-pago/`

## Validación real con la credencial actual

### APIs que sí responden y aportan datos nuevos

#### 1. Variantes

- Endpoint: `GET /sistema/api/v1/variante/`
- Estado real: `200`
- Resultado real en este tenant: `0` registros
- Valor técnico:
  - Permitiría modelar atributos como color, talla o dimensión.
  - Hoy no aporta valor inmediato porque el tenant no tiene variantes cargadas.
- Decisión:
  - Integración opcional, baja prioridad.

#### 2. Guías de remisión

- Endpoint: `GET /sistema/api/v1/inventario/guia/`
- Estado real: `200`
- Resultado real en este tenant: `38` registros
- Campos observados:
  - `id`
  - `fecha_emision`
  - `fecha_inicio`
  - `fecha_fin`
  - `numero_documento`
  - `transportista_id`
  - `bodega_id`
  - `estado`
  - `destinatario`
  - `electronico`
  - `placa`
  - `descripcion`
- Valor analítico:
  - Añade la capa logística que hoy falta entre inventario y entrega.
  - Permite medir despacho, trazabilidad, transporte y salida física de mercadería.
- Relaciones probables:
  - `bodega_id -> core.bodegas.id`
  - `transportista_id -> core.personas.id`
  - `destinatario.destinatario_id -> core.personas.id`
  - `destinatario.documento_id -> core.documentos.id` cuando exista correspondencia
  - `destinatario.detalle[].producto_id -> core.productos.id`
- Prioridad sugerida:
  - Alta.

#### 3. Cuentas bancarias

- Endpoint: `GET /sistema/api/v1/banco/cuenta/`
- Estado real: `200`
- Resultado real en este tenant: `5` registros
- Campos observados:
  - `id`
  - `nombre`
  - `numero`
  - `tipo_cuenta`
  - `cuenta_contable`
  - `saldo_inicial`
  - `fecha_corte`
  - `estado`
- Valor analítico:
  - Completa la dimensión bancaria, hoy ausente en el modelo.
  - Permite conectar tesorería con contabilidad.
- Relaciones probables:
  - `cuenta_contable -> core.cuentas_contables.id`
- Prioridad sugerida:
  - Alta.

#### 4. Movimientos bancarios

- Endpoint: `GET /sistema/api/v1/banco/movimiento/`
- Estado real: `200`
- Resultado real en este tenant: `2.751` registros
- Campos observados:
  - `id`
  - `tipo_registro`
  - `tipo`
  - `fecha_emision`
  - `numero_comprobante`
  - `persona`
  - `cuenta_bancaria_id`
  - `detalles`
- Valor analítico:
  - Añade flujo de caja y bancos, hoy no visible en el dashboard actual.
  - Permite conciliación entre cobros, movimientos bancarios y asientos.
- Relaciones probables:
  - `persona -> core.personas.id`
  - `cuenta_bancaria_id -> nueva tabla banco_cuentas.id`
  - `detalles[].cuenta_id -> core.cuentas_contables.id`
  - `detalles[].centro_costo_id -> core.centros_costo.id`
- Prioridad sugerida:
  - Muy alta.

### APIs documentadas pero con validación incierta

#### 5. Transacciones

- Endpoint documentado: `GET /sistema/api/v1/registro/transaccion/`
- Estado real observado: timeout a `90 s`
- Interpretación:
  - La API existe en documentación pública, pero no quedó validada operativamente en este tenant dentro de un tiempo razonable.
  - Puede requerir paginación específica, filtros o simplemente tener un volumen alto.
- Potencial valor:
  - Podría servir como puente entre documentos, cobros, caja/POS y eventos de registro.
- Decisión:
  - No integrarla todavía como fuente productiva.
  - Hacer una prueba focalizada aparte, con paginación o filtros controlados.

### APIs documentadas pero no confiables hoy

#### 6. RRHH Roles

- Endpoint documentado: `GET /sistema/api/v1/rrhh/rol-pago/?cedula=x&periodo=x&anio=x&mes=x`
- Estado real observado: `500`
- Interpretación:
  - El endpoint existe, pero hoy no es estable para este tenant o presenta un error interno del backend.
- Decisión:
  - No considerar integración por ahora.

## Contraste con el modelo actual

### Lo ya cubierto

Actualmente ya tenemos una base sólida para:

- ventas y documentos
- detalle de productos por documento
- cobros
- inventario y movimientos
- asientos contables
- personas, bodegas, marcas, categorías, centros de costo, cuentas contables

### Lo que falta y sí podemos obtener ahora

Las APIs nuevas con mayor valor real y validación positiva son:

1. `inventario/guia`
2. `banco/cuenta`
3. `banco/movimiento`

Estas tres expanden el modelo hacia:

- logística y despacho
- tesorería y bancos
- conciliación entre operación, caja y contabilidad

### Lo que hoy no aporta o no está listo

- `variante`: responde, pero hoy viene vacío en este tenant
- `registro/transaccion`: documentado, pero no quedó validado
- `rrhh/rol-pago`: documentado, pero respondió con error `500`

## Qué podemos obtener adicionalmente

### 1. Capa logística real

Con `inventario/guia` podemos obtener:

- guías emitidas
- destinatarios
- transportistas
- bodegas de salida
- detalle de productos despachados
- fechas de inicio/fin de traslado
- estado documental y electrónico

Esto habilita análisis de:

- despacho vs facturación
- productos facturados sin salida logística
- salidas por bodega
- trazabilidad por cliente/destinatario

### 2. Capa de tesorería

Con `banco/cuenta` y `banco/movimiento` podemos obtener:

- catálogo real de cuentas bancarias
- movimientos de ingreso/egreso por banco
- persona asociada al movimiento
- detalle contable por movimiento
- relación entre banco y cuenta contable

Esto habilita análisis de:

- conciliación bancaria
- flujo de tesorería
- comparación entre cobros registrados y movimientos bancarios
- actividad bancaria por persona, cuenta y centro de costo

### 3. Eventual capa transaccional intermedia

Si `registro/transaccion` se logra estabilizar, podría aportar:

- una visión más granular del evento transaccional
- mayor trazabilidad POS/caja/registro
- posible puente entre documento, cobro y cruce

Esto es una inferencia a partir de la documentación pública y del nombre del recurso; hoy no quedó validado con respuesta funcional.

## Recomendación técnica

### Prioridad 1

Integrar:

- `inventario/guia`
- `banco/cuenta`
- `banco/movimiento`

Porque:

- responden hoy con la credencial actual
- agregan dominios nuevos que el modelo actual no cubre
- tienen relaciones claras con tablas existentes
- elevan mucho el valor analítico del dashboard

### Prioridad 2

Dejar preparada, pero no priorizar, la tabla de:

- `variante`

Porque:

- el esfuerzo es bajo
- hoy no hay datos
- puede activarse en el futuro sin rediseño mayor

### Prioridad 3

Hacer un spike técnico aparte para:

- `registro/transaccion`

Porque:

- su valor potencial es bueno
- pero hoy no hay evidencia operativa suficiente

### Fuera de alcance por ahora

- `rrhh/rol-pago`

Porque:

- hoy respondió con error del backend
- no es confiable como fuente productiva

## Conclusión

Sí hay APIs públicas adicionales aprovechables con la misma credencial actual.

Las que realmente justifican una nueva integración son:

- `inventario/guia`
- `banco/cuenta`
- `banco/movimiento`

Con eso el modelo dejaría de ser solo comercial/inventario/contable y pasaría a cubrir también:

- logística
- tesorería
- conciliación operativa-financiera

Esa es la expansión con mayor retorno técnico hoy.
