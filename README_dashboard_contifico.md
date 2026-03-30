# Dashboard Contifico

Dashboard estatico en `HTML + CSS + JS` preparado para login con Supabase Auth. El refresh ya no depende de `localhost`: la pagina autentica al usuario, consume snapshot privado desde Supabase y puede disparar funciones cloud que actualizan la base maestra y regeneran la analitica.

## Arquitectura operativa

1. El usuario inicia sesion en Supabase Auth.
2. La pagina carga el snapshot privado desde `dashboard-snapshot`.
3. El usuario pulsa `Refresh rapido` o `Refresh completo`.
4. `dashboard/app.js` llama a `contifico-refresh` en Supabase Edge Functions.
5. La funcion dispara `.github/workflows/contifico-cloud-refresh.yml`.
6. GitHub Actions ejecuta:
   - `contifico_pg_backfill.py`
   - `export_dashboard_data.py`
7. El workflow escribe en Supabase, actualiza `app.snapshot_assets` y hace commit de `dashboard/data/*.json`.
8. El hosting estatico publica el snapshot nuevo.

## Archivos principales

- `dashboard/index.html`
- `dashboard/styles.css`
- `dashboard/app.js`
- `dashboard/charts.js`
- `dashboard/config.js`
- `dashboard/data/*.json`
- `supabase/functions/dashboard-snapshot/index.ts`
- `supabase/functions/contifico-refresh/index.ts`
- `supabase/functions/contifico-refresh-status/index.ts`
- `.github/workflows/contifico-cloud-refresh.yml`

## Configuracion cloud del frontend

El frontend lee `dashboard/config.js`:

```js
window.CONTIFICO_CONFIG = {
  snapshotBase: "./data",
  snapshotApiUrl: "https://<project-ref>.supabase.co/functions/v1/dashboard-snapshot",
  refreshApiUrl: "https://<project-ref>.supabase.co/functions/v1/contifico-refresh",
  refreshStatusUrl: "https://<project-ref>.supabase.co/functions/v1/contifico-refresh-status",
  supabaseUrl: "https://<project-ref>.supabase.co",
  supabaseAnonKey: "<SUPABASE_ANON_KEY>",
};
```

## Secrets necesarios

### En GitHub Actions

- `CONTIFICO_AUTHORIZATION`
- `SUPABASE_DB_URL`

### En Supabase Edge Functions

- `GITHUB_WORKFLOW_TOKEN`
- `GITHUB_OWNER`
- `GITHUB_REPO`
- `GITHUB_WORKFLOW_FILE`
- `GITHUB_REF`

## Publicacion

Publica `dashboard/` en Vercel o Netlify. No abras `index.html` con `file://`.

## Nota

- La base maestra del sistema es Supabase Postgres.
- El snapshot privado queda replicado en `app.snapshot_assets`.
