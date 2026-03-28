# Revision tecnica del Manual API V2 de Contifico

Fecha de revision: 2026-03-28

Documento revisado: `Manual API V2 1.docx`

## 1. Conclusion operativa

El Word no se puede usar como fuente unica de verdad para integrar la API. Sirve para levantar el inventario funcional, pero no para confiar ciegamente en sus hipervinculos ni en todos sus ejemplos JSON.

Conclusiones principales:

- Los endpoints visibles del manual si permiten reconstruir un mapa funcional razonable de la API v2.
- Los hipervinculos incrustados del Word son poco confiables y en muchos casos apuntan a recursos incorrectos.
- Para integrar bien conviene usar como canon:
  1. El texto visible del manual.
  2. La documentacion oficial publica de Contifico como apoyo para nombres de campos y dependencias de catalogos.
  3. Un criterio de normalizacion propio para URLs, fechas y payloads.

## 2. Calidad del manual

Hallazgos cuantitativos al revisar el `.docx` por dentro:

- 43 parrafos contienen URLs visibles.
- 49 fragmentos tienen hipervinculos incrustados.
- 31 veces el texto visible no coincide con el destino real del hipervinculo.
- 13 destinos apuntan a `contifico.testcontifico.com`, no al ambiente productivo.
- 7 destinos apuntan a endpoints `v1`.
- 7 destinos contienen rutas claramente rotas como `itpbodega=nombre`.
- 1 destino era un `mailto:` incrustado dentro de un ejemplo JSON.

Implicacion practica:

- No hay que seguir los enlaces del Word.
- Hay que extraer y usar el texto visible, no el target del hipervinculo.

Ejemplos criticos detectados:

- Visible: `https://api.contifico.com/sistema/api/v2/producto/?page=2`
  Destino incrustado: mezcla de `persona/personas/` y `testcontifico.com/api/v2/inventario/productos/?page=2`
- Visible: `https://api.contifico.com/sistema/api/v2/persona?estado=A`
  Destino incrustado: `mailto:agomez_ee@nomail.com?estado=A`
- Visible: `https://api.contifico.com/sistema/api/v2/documento/?tipo=FAC`
  Destino incrustado: `https://api.contifico.com/sistema/api/v2/movimiento-inventario/?tipo=FAC=`

## 3. Normalizacion recomendada

Antes de programar la integracion, conviene fijar estas reglas:

- Usar siempre `https://api.contifico.com`.
- No usar ninguna URL `http://`.
- Para endpoints de coleccion, usar barra final `/`.
- No copiar parametros con espacios, por ejemplo `? centro_costo=...` debe quedar `?centro_costo=...`.
- No copiar ejemplos JSON del Word literalmente: tienen comillas tipograficas, comas sobrantes y algunos strings con espacios basura.

Verificacion directa hecha sobre la API:

- `https://api.contifico.com/sistema/api/v2/persona` responde `301` hacia `/sistema/api/v2/persona/`
- `https://api.contifico.com/sistema/api/v2/documento/tickets` responde `301` hacia `/sistema/api/v2/documento/tickets/`

Inferencia razonable:

- Aunque el manual mezcla rutas con y sin slash final, conviene consumir siempre las colecciones con slash para evitar redirecciones innecesarias.

## 4. Inventario depurado de endpoints v2 del manual

### 4.1 Productos

Estado en el manual: `Terminado`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/producto/?page={n}`
- `GET https://api.contifico.com/sistema/api/v2/producto/?categoria_id={categoria_id}`
- `GET https://api.contifico.com/sistema/api/v2/producto/?estado={A|I}`
- `GET https://api.contifico.com/sistema/api/v2/producto/?fecha_inicial={AAAA-MM-DD}&fecha_final={AAAA-MM-DD}`
- `GET https://api.contifico.com/sistema/api/v2/producto/{bodega_id}/stock/`
- `POST https://api.contifico.com/sistema/api/v2/producto/`
- `PUT https://api.contifico.com/sistema/api/v2/producto/{id_integracion}/`

Dependencias detectadas:

- `categoria_id`
- `categoria_comisariato_id`
- `marca_id`
- `bodega_id` para stock

Correcciones y riesgos:

- El Word usa `categoria_comisariato` en vez de `categoria_comisariato_id`. Esto no coincide con la documentacion oficial publica de v1 y es de alto riesgo. Recomiendo estandarizar internamente como `categoria_comisariato_id`.
- Los ejemplos usan `http://api.contifico.com/...`; debe cambiarse a `https://api.contifico.com/...`.
- Los ejemplos tienen comillas tipograficas en `marca_id` y `marca_nombre`, lo que invalida el JSON.
- Hay valores con espacios sobrantes, por ejemplo `" zXrr2TS6Kd7Zn68M"`; hay que aplicar `trim`.
- El campo `codigo` tiene limite de 25 caracteres segun el manual.
- Si `habilitar_comiexpress` esta activo, el payload cambia y aparecen campos como `departamento`, `familia`, `jerarquia`, `indicador_peso`.

### 4.2 Personas

Estado en el manual: `Terminado`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/persona/?page={n}`
- `GET https://api.contifico.com/sistema/api/v2/persona/?tipo={N|J|I|P}`
- `GET https://api.contifico.com/sistema/api/v2/persona/?estado={A|I}`
- `GET https://api.contifico.com/sistema/api/v2/persona/?es_cliente={0|1}`
- `GET https://api.contifico.com/sistema/api/v2/persona/?es_proveedor={0|1}`
- `GET https://api.contifico.com/sistema/api/v2/persona/?fecha_inicial={AAAA-MM-DD}&fecha_final={AAAA-MM-DD}`
- `GET https://api.contifico.com/sistema/api/v2/persona/{persona_id}`
- `GET https://api.contifico.com/sistema/api/v2/persona/?categoria_id={categoria_id}`
- `POST https://api.contifico.com/sistema/api/v2/persona/?pos={pos_token}`

Dependencias detectadas:

- `categoria_id`
- `pos` para creacion

Correcciones y riesgos:

- En los hipervinculos aparecen rutas `persona/personas` y hasta `personas` en plural. No hay que usar eso como base canonica.
- El manual visible usa `fecha_inicial` y `fecha_final`; los links ocultos mezclan `fecha_inicio` y `fecha_fin`. Conviene tomar como canon `fecha_inicial` y `fecha_final`.
- Los valores booleanos del JSON deben enviarse como booleanos reales, no como strings.
- Debe venir al menos uno de `es_cliente=true` o `es_proveedor=true`.

### 4.3 Movimiento de inventario

Estado en el manual: `Terminado`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/movimiento-inventario/?page={n}`
- `GET https://api.contifico.com/sistema/api/v2/movimiento-inventario/?fecha_inicial={AAAA-MM-DD}&fecha_final={AAAA-MM-DD}`
- `GET https://api.contifico.com/sistema/api/v2/movimiento-inventario/?tipo={ING|EGR|TRA|AJU}`
- `GET https://api.contifico.com/sistema/api/v2/movimiento-inventario/?bodega_id={bodega_id}`
- `POST https://api.contifico.com/sistema/api/v2/movimiento-inventario/`

Dependencias detectadas:

- `bodega_id`
- `bodega_destino_id`
- `producto_id`
- `unidad` o identificador de unidad en el detalle

Correcciones y riesgos:

- En los ejemplos JSON aparece `"generar_asiento": "true"`. Debe tratarse como booleano `true`, no como string.
- Los ejemplos de detalle tienen comas sobrantes antes de cerrar el objeto.
- El campo `unidad` trae IDs con espacios al final. Hay que normalizar con `trim`.
- El tipo `TRA` requiere `bodega_destino_id`.
- El tipo `ING` requiere `precio` en el detalle; esto si esta alineado con la documentacion oficial publica de v1.

### 4.4 Documentos

Estado en el manual: `en desarrollo la parte de creacion`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/documento/?page={n}`
- `GET https://api.contifico.com/sistema/api/v2/documento/?tipo={codigo_tipo}`
- `GET https://api.contifico.com/sistema/api/v2/documento/?tipo_registro={CLI|PRO}`
- `GET https://api.contifico.com/sistema/api/v2/documento/?fecha_emision={AAAA-MM-DD}`
- `GET https://api.contifico.com/sistema/api/v2/documento/?fecha_inicial={AAAA-MM-DD}&fecha_final={AAAA-MM-DD}`
- `GET https://api.contifico.com/sistema/api/v2/documento/{documento_id}`
- `GET https://api.contifico.com/sistema/api/v2/documento/?estado={P|C|G|A|E|F|R|T|Z}`
- `POST https://api.contifico.com/sistema/api/v2/documento/`

Dependencias detectadas:

- `producto_id`
- `caja_id`
- `cliente`
- `vendedor`
- `persona` para `tipo_registro=PRO` o `EMP`
- `cuenta` dentro del detalle
- `pos`

Correcciones y riesgos:

- El modulo no esta estable del todo para creacion segun el propio manual.
- En la documentacion publica de v1 la ruta de listado es `registro/documento/`; en el Word v2 aparece `documento/`. Esto parece ser un cambio intencional de version, pero los hipervinculos del Word siguen mezclando ambas rutas.
- Los detalles usan `producto_id`, lo cual relaciona directamente este modulo con `producto`.
- El ejemplo CLI usa objetos `cliente` y `vendedor`; el ejemplo PRO/EMP usa objeto `persona`.
- El campo `cuenta` aparece en el detalle del documento v2 del Word, pero no esta claramente documentado en la referencia publica v1. Debe validarse contra respuestas reales del tenant.
- Los estados documentados son: `P`, `C`, `G`, `A`, `E`, `F`, `R`, `T`, `Z`.

### 4.5 Unidad

Estado en el manual: `En proceso`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/unidad/?page={n}`
- `GET https://api.contifico.com/sistema/api/v2/unidad/?search={texto}`
- `GET https://api.contifico.com/sistema/api/v2/unidad/{unidad_id}/`

Dependencias detectadas:

- Este catalogo se cruza con `movimiento-inventario.detalles[].unidad`

Correcciones y riesgos:

- Los hipervinculos incrustados de esta seccion apuntan a `persona/personas` y a `testcontifico`, asi que hay que ignorarlos por completo.
- La seccion esta marcada como `En proceso`, por lo que hay mayor riesgo de cambios o respuestas incompletas.

### 4.6 Asientos contables

Estado en el manual: `En proceso`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/contabilidad/asiento/?page={n}`
- `GET https://api.contifico.com/sistema/api/v2/contabilidad/asiento/?fecha_inicial={DD/MM/AAAA}&fecha_final={DD/MM/AAAA}`
- `GET https://api.contifico.com/sistema/api/v2/contabilidad/asiento/?centro_costo={centro_costo_id}`

Dependencias detectadas:

- `cuenta_id`
- `centro_costo_id`

Correcciones y riesgos:

- El ejemplo del Word tiene un espacio invalido en la query: `? centro_costo=...`; debe ser `?centro_costo=...`
- La documentacion publica de v1 expone consulta por ID y creacion, pero no lista publica con filtros como el Word de v2. Por eso esta seccion conviene tratarla como parcialmente validada.
- Las fechas aqui usan `DD/MM/AAAA`, no `AAAA-MM-DD`.

### 4.7 Periodos contables

Estado en el manual: `Terminado`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/contabilidad/periodo/?page={n}`
- `GET https://api.contifico.com/sistema/api/v2/contabilidad/periodo/?fecha_inicial={DD/MM/AAAA}&fecha_final={DD/MM/AAAA}`
- `GET https://api.contifico.com/sistema/api/v2/contabilidad/periodo/?estado={AB|CE}`
- `GET https://api.contifico.com/sistema/api/v2/contabilidad/periodo/{periodo_id}`

Correcciones y riesgos:

- Los hipervinculos del Word en esta seccion tambien estan contaminados con rutas rotas tipo `itpbodega=nombre`.
- La descripcion del filtro de fecha habla de fecha de creacion, no de fecha operativa.

### 4.8 Boleteria

Estado en el manual: `Terminado`

Endpoints visibles depurados:

- `GET https://api.contifico.com/sistema/api/v2/documento/tickets/`
- `GET https://api.contifico.com/sistema/api/v2/documento/tickets/{documento_id}`

Dependencias detectadas:

- `documento_id`

Correcciones y riesgos:

- Para explotar boleteria primero hay que tener el universo de documentos, luego expandir solo los documentos relevantes.

## 5. APIs relacionadas que no estan desarrolladas en el Word pero si son necesarias

Para relacionar bien los IDs del manual hacen falta catalogos auxiliares. No vienen bien explicados en el Word v2, pero si aparecen en la documentacion oficial publica:

- Categoria: necesaria por `producto.categoria_id` y `persona.categoria_id`
- Bodega: necesaria por `producto/{bodega_id}/stock/`, `movimiento-inventario.bodega_id` y `documento.bodega_id` en filtros historicos de v1
- Marca: necesaria por `producto.marca_id`
- Cuenta contable: necesaria por `asiento.detalles[].cuenta_id` y probablemente por `documento.detalles[].cuenta`
- Centro de costo: necesaria por `asiento` y su filtro `centro_costo`
- Cobros: util para expandir los pagos de un documento si despues hace falta auditar `cobros`

## 6. Mapa de relaciones entre recursos

Relacion funcional recomendada:

- `categoria` -> `producto`
- `categoria` -> `persona`
- `marca` -> `producto`
- `bodega` -> `producto stock`
- `bodega` -> `movimiento-inventario`
- `bodega` -> `documento` si luego se usan filtros historicos o campos heredados
- `unidad` -> `movimiento-inventario.detalles[].unidad`
- `producto` -> `movimiento-inventario.detalles[].producto_id`
- `producto` -> `documento.detalles[].producto_id`
- `persona` -> `documento.persona` para `PRO|EMP`
- `persona` -> `documento.cliente`
- `persona` -> `documento.vendedor`
- `documento` -> `documento/tickets/{documento_id}`
- `cuenta-contable` -> `asiento.detalles[].cuenta_id`
- `centro-costo` -> `asiento.detalles[].centro_costo_id`

## 7. Orden recomendado de extraccion

Para una integracion estable recomiendo este orden:

1. Catalogos base:
   - `categoria`
   - `marca`
   - `bodega`
   - `unidad`
   - `cuenta-contable`
   - `centro-costo`
2. Maestros de negocio:
   - `persona`
   - `producto`
3. Operacion:
   - `movimiento-inventario`
   - `documento`
4. Expansion por detalle dependiente:
   - `documento/tickets/{documento_id}`
5. Contabilidad:
   - `contabilidad/periodo`
   - `contabilidad/asiento`

Razon:

- Asi primero resuelves todos los IDs de referencia.
- Luego cargas los recursos que contienen esos IDs.
- Al final expandes tickets y contabilidad, que dependen de documentos o de catalogos contables previos.

## 8. Consultas reformuladas recomendadas para extraccion

Estas son las consultas que yo tomaria como base operativa para extraer informacion sin heredar los errores del Word.

### 8.1 Extraccion incremental de personas

`GET /sistema/api/v2/persona/?fecha_inicial=2026-03-01&fecha_final=2026-03-28`

Opcionalmente combinar con:

- `estado=A`
- `es_cliente=1`
- `es_proveedor=1`

Nota:

- La combinacion de filtros es una inferencia razonable porque el manual los presenta como query params independientes del mismo recurso.

### 8.2 Extraccion incremental de productos

`GET /sistema/api/v2/producto/?fecha_inicial=2026-03-01&fecha_final=2026-03-28`

Opcionalmente combinar con:

- `estado=A`
- `categoria_id={id}`

### 8.3 Extraccion de stock por bodega

`GET /sistema/api/v2/producto/{bodega_id}/stock/`

Nota:

- Aunque el texto del manual dice "stock por bodega", la ruta usa el ID entre `producto` y `stock`. Operativamente el texto manda mas que el link oculto, pero esta forma conviene validarla con respuestas reales del tenant porque el nombre del segmento puede inducir confusion.

### 8.4 Extraccion incremental de movimientos

`GET /sistema/api/v2/movimiento-inventario/?fecha_inicial=2026-03-01&fecha_final=2026-03-28`

Opcionalmente combinar con:

- `tipo=ING|EGR|TRA|AJU`
- `bodega_id={id}`

### 8.5 Extraccion incremental de documentos

`GET /sistema/api/v2/documento/?fecha_inicial=2026-03-01&fecha_final=2026-03-28`

Opcionalmente combinar con:

- `tipo_registro=CLI`
- `tipo=FAC`
- `estado=P`

Para enriquecimiento:

- listar documentos por rango
- tomar cada `documento_id`
- expandir con `GET /sistema/api/v2/documento/{documento_id}`
- si aplica boleteria, llamar `GET /sistema/api/v2/documento/tickets/{documento_id}`

### 8.6 Extraccion de asientos

`GET /sistema/api/v2/contabilidad/asiento/?fecha_inicial=01/03/2026&fecha_final=28/03/2026`

Opcionalmente combinar con:

- `centro_costo={id}`

### 8.7 Extraccion de periodos

`GET /sistema/api/v2/contabilidad/periodo/?estado=AB`

o

`GET /sistema/api/v2/contabilidad/periodo/?fecha_inicial=01/03/2026&fecha_final=28/03/2026`

## 9. Riesgos tecnicos que hay que blindar en el conector

Si se va a construir un cliente o pipeline, recomiendo blindar estos puntos desde el codigo:

- Forzar `https`.
- Forzar slash final en endpoints de coleccion.
- Normalizar espacios y comillas antes de guardar payloads de ejemplo.
- Tratar fechas por modulo, no globalmente:
  - `AAAA-MM-DD` para `producto`, `persona`, `movimiento-inventario`, `documento`
  - `DD/MM/AAAA` para `asiento` y `periodo`
- Validar que los booleans salgan como booleanos reales.
- Paginar hasta vaciar resultados.
- Guardar los IDs de integracion tal como llegan, sin convertirlos.
- Implementar logs por recurso y por pagina para aislar errores de filtros mal aceptados por el backend.

## 10. Fuentes usadas para contraste

Documentacion oficial publica de Contifico:

- Introduccion y autenticacion: https://contifico.github.io/
- Producto: https://contifico.github.io/inventario/producto/
- Movimientos: https://contifico.github.io/inventario/movimientos/
- Documento: https://contifico.github.io/registro/documento/
- Cobros: https://contifico.github.io/registro/cobros/
- Marca: https://contifico.github.io/inventario/marca/
- Bodega: https://contifico.github.io/inventario/bodega/
- Categoria: https://contifico.github.io/inventario/categoria/
- Cuentas contables: https://contifico.github.io/contabilidad/cuentas/
- Centros de costo: https://contifico.github.io/contabilidad/centrocostos/

## 11. Siguiente paso recomendado

El siguiente paso util ya no es seguir leyendo el Word, sino armar una especificacion tecnica interna con:

- endpoint canonico
- metodo
- formato de fecha
- filtros soportados
- dependencias previas
- mapeo de respuesta
- regla de paginacion
- validaciones de payload

Con eso ya se puede empezar a implementar el conector con menos riesgo.
