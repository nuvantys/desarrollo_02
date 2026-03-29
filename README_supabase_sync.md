# Sync a Supabase

Este proyecto mantiene `contifico_backfill` como base maestra local y replica a Supabase como destino remoto. La idea es no romper el pipeline actual: primero se actualiza localmente desde Contifico, luego se sincroniza a Supabase en un paso aparte y explícito.

## Diseño

- Fuente de verdad operativa: PostgreSQL local `contifico_backfill`
- Destino remoto: Supabase Postgres
- Esquemas preservados: `meta`, `raw`, `core`, `reporting`
- Método de carga: `COPY` entre PostgreSQL local y PostgreSQL remoto
- Validaciones post-carga:
  - recreación de DDL si no existe
  - truncado seguro del destino antes de recargar
  - validación de la FK `documento_cobros -> banco_cuentas`
  - recreación de vistas `reporting`

## Por qué así

- No mezcla refresh operativo con publicación remota.
- Evita romper el dashboard local si Supabase falla o se satura.
- Conserva la lógica relacional completa del modelo actual.
- Permite decidir si subir o no `raw.resource_rows`, que suele ser la capa más pesada y menos necesaria para consumo web.

## Recomendación operativa

- Para pruebas y uso diario: sincronizar `core + meta + reporting`
- Para auditoría profunda: añadir `--include-raw`
- Para destino Supabase: usar conexión PostgreSQL nativa con SSL

## Variables y ejecución

Variables locales:

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

Sincronización estándar:

```powershell
python .\supabase_sync.py `
  --source-db-name contifico_backfill `
  --report-out .\supabase_sync_report.md
```

Sincronización incluyendo capa raw:

```powershell
python .\supabase_sync.py `
  --source-db-name contifico_backfill `
  --include-raw `
  --report-out .\supabase_sync_report.md
```

Si no quieres truncar el destino antes de copiar:

```powershell
python .\supabase_sync.py `
  --source-db-name contifico_backfill `
  --no-truncate-target
```

## Alcance actual de la réplica

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

## Lectura técnica

- `meta` permite que la réplica conserve bitácora, watermarks y trazabilidad.
- `reporting` se reconstruye en el destino, no se copia físicamente.
- La sincronización usa el mismo modelo relacional que el dashboard local, así que no requiere rehacer joins ni renombrar capas.
- Supabase seguirá siendo PostgreSQL; por tanto, el diseño actual de FKs, vistas y tipos `jsonb`, `numeric`, `date` y `timestamptz` es compatible.

## Fuentes oficiales consultadas

- Supabase Import data: https://supabase.com/docs/guides/database/import-data

La recomendación de usar conexión PostgreSQL directa y cargas por `COPY`/herramientas de base se apoya en la guía oficial de importación de Supabase, que desaconseja usar la API para importaciones grandes y recomienda métodos de base de datos para cargas voluminosas.
