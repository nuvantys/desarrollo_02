# Sync entre PostgreSQL y Supabase

Este utilitario queda como herramienta auxiliar para copiar tablas entre motores PostgreSQL compatibles. Ya no forma parte del flujo principal del dashboard, porque la base maestra operativa vive directamente en Supabase.

## Diseño

- Fuente de verdad operativa actual: Supabase Postgres
- Uso de este script: sincronizaciones auxiliares o migraciones puntuales entre entornos PostgreSQL
- Esquemas preservados: `meta`, `raw`, `core`, `reporting`
- Metodo de carga: `COPY` entre dos instancias PostgreSQL
- Validaciones post-carga:
  - recreacion de DDL si no existe
  - truncado seguro del destino antes de recargar
  - validacion de la FK `documento_cobros -> banco_cuentas`
  - recreacion de vistas `reporting`

## Por que asi

- No mezcla refresh operativo con una migracion puntual entre entornos.
- Sigue siendo util para replicar o mover datos entre dos motores PostgreSQL cuando haga falta.
- Conserva la logica relacional completa del modelo actual.
- Permite decidir si subir o no `raw.resource_rows`, que suele ser la capa mas pesada y menos necesaria para consumo web.

## Recomendacion operativa

- Para pruebas y uso diario: sincronizar `core + meta + reporting`
- Para auditoria profunda: anadir `--include-raw`
- Para destino Supabase: usar conexion PostgreSQL nativa con SSL

## Variables y ejecucion

Origen PostgreSQL opcional:

```powershell
$env:PGHOST = '127.0.0.1'
$env:PGPORT = '5432'
$env:PGUSER = 'postgres'
$env:PGPASSWORD = 'postgres'
```

Destino Supabase:

```powershell
$env:SUPABASE_DB_URL = 'postgresql://postgres.<ref>:<password>@<host>:5432/postgres?sslmode=require'
```

Sincronizacion estandar:

```powershell
python .\supabase_sync.py `
  --source-db-name postgres `
  --report-out .\supabase_sync_report.md
```

Sincronizacion incluyendo capa raw:

```powershell
python .\supabase_sync.py `
  --source-db-name postgres `
  --include-raw `
  --report-out .\supabase_sync_report.md
```

Si no quieres truncar el destino antes de copiar:

```powershell
python .\supabase_sync.py `
  --source-db-name postgres `
  --no-truncate-target
```

## Alcance actual de la replica

Se sincronizan estas tablas:

- `core.cuentas_contables`
- `core.categorias`
- `core.bodegas`
- `core.marcas`
- `core.unidades`
- `core.centros_costo`
- `core.periodos`
- `core.banco_cuentas`
- `core.personas`
- `core.productos`
- `core.guias`
- `core.guia_destinatarios`
- `core.guia_detalles`
- `core.banco_movimientos`
- `core.banco_movimiento_detalles`
- `core.movimientos`
- `core.movimiento_detalles`
- `core.documentos`
- `core.documento_detalles`
- `core.documento_cobros`
- `core.tickets_documentos`
- `core.tickets_detalles`
- `core.tickets_items`
- `core.asientos`
- `core.asiento_detalles`
- `meta.extract_runs`
- `meta.load_metrics`
- `meta.watermarks`

Opcional:

- `raw.resource_rows`

## Lectura tecnica

- `meta` permite que la replica conserve bitacora, watermarks y trazabilidad.
- `reporting` se reconstruye en el destino, no se copia fisicamente.
- La sincronizacion usa el mismo modelo relacional que el dashboard, asi que no requiere rehacer joins ni renombrar capas.
- Supabase sigue siendo PostgreSQL; por tanto, el diseño actual de FKs, vistas y tipos `jsonb`, `numeric`, `date` y `timestamptz` es compatible.

## Fuentes oficiales consultadas

- Supabase Import data: https://supabase.com/docs/guides/database/import-data

La recomendacion de usar conexion PostgreSQL directa y cargas por `COPY`/herramientas de base se apoya en la guia oficial de importacion de Supabase, que desaconseja usar la API para importaciones grandes y recomienda metodos de base de datos para cargas voluminosas.
