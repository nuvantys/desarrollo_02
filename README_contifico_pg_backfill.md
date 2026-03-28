# Contifico PostgreSQL Backfill

Backfill historico completo de Contifico V2 hacia PostgreSQL local, con capa `raw`, modelo `core`, metadata en `meta` y vistas de validacion en `reporting`.

## Requisitos

- Python 3.11+
- PostgreSQL accesible
- Variables de entorno:
  - `CONTIFICO_AUTHORIZATION`
  - `PGUSER`
  - `PGPASSWORD`
  - `PGHOST` opcional, default `127.0.0.1`
  - `PGPORT` opcional, default `5432`
  - `PGMAINTENANCE_DB` opcional, default `postgres`

## Ejecucion

PowerShell:

```powershell
$env:CONTIFICO_AUTHORIZATION = 'tu_token'
$env:PGUSER = 'postgres'
$env:PGPASSWORD = 'tu_password'

python .\contifico_pg_backfill.py `
  --mode backfill `
  --db-name contifico_backfill `
  --report-out .\final_report.md `
  --save-raw
```

## Objetos creados

- Esquemas:
  - `meta`
  - `raw`
  - `core`
  - `reporting`
- Tablas de control:
  - `meta.extract_runs`
  - `meta.load_metrics`
  - `meta.watermarks`
- Tabla raw:
  - `raw.resource_rows`
- Tablas core:
  - `categorias`
  - `bodegas`
  - `marcas`
  - `unidades`
  - `cuentas_contables`
  - `centros_costo`
  - `periodos`
  - `personas`
  - `productos`
  - `movimientos`
  - `movimiento_detalles`
  - `documentos`
  - `documento_detalles`
  - `documento_cobros`
  - `tickets_documentos`
  - `tickets_detalles`
  - `tickets_items`
  - `asientos`
  - `asiento_detalles`

## Salidas

- Datos historicos dentro de PostgreSQL
- Vistas en `reporting` para resumen de carga, cobertura temporal y salud relacional
- Informe Markdown en la ruta indicada por `--report-out`

## Notas

- El backfill historico no depende de filtros de fecha del backend; usa paginacion completa.
- Cuando la API devuelve referencias que no existen en el catalogo origen, el loader crea placeholders controlados para conservar integridad referencial y luego los sustituye si aparece el maestro real.
- Los campos sin catalogo validado se guardan como atributos simples, no como FK.
