# Dashboard Contifico

Dashboard estatico en `HTML + CSS + JS` alimentado desde snapshots JSON publicados en la nube. El refresh ya no depende de `localhost`: la pagina dispara funciones cloud en Supabase, esas funciones despachan un workflow de GitHub Actions, el workflow actualiza Supabase y republlica `dashboard/data/*.json`.

## Arquitectura operativa

1. La pagina carga `dashboard/data/*.json`
2. El usuario pulsa `Refresh rapido` o `Refresh completo`
3. `dashboard/app.js` llama a `contifico-refresh` en Supabase Edge Functions
4. La funcion dispara `.github/workflows/contifico-cloud-refresh.yml`
5. GitHub Actions ejecuta:
   - `contifico_pg_backfill.py`
   - `export_dashboard_data.py`
6. El workflow escribe en Supabase y hace commit de `dashboard/data/*.json`
7. El hosting estatico publica el snapshot nuevo

## Archivos principales

- `dashboard/index.html`
- `dashboard/styles.css`
- `dashboard/app.js`
- `dashboard/charts.js`
- `dashboard/config.js`
- `dashboard/data/*.json`
- `supabase/functions/contifico-refresh/index.ts`
- `supabase/functions/contifico-refresh-status/index.ts`
- `.github/workflows/contifico-cloud-refresh.yml`

## Configuracion cloud del frontend

El frontend lee [config.js](d:/Temp/Trabajos/manual/desarrollo_02/dashboard/config.js):

```js
window.CONTIFICO_CONFIG = {
  snapshotBase: "./data",
  refreshApiUrl: "https://<project-ref>.supabase.co/functions/v1/contifico-refresh",
  refreshStatusUrl: "https://<project-ref>.supabase.co/functions/v1/contifico-refresh-status",
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

Publica `dashboard/` en GitHub Pages, Netlify o Vercel. No abras `index.html` con `file://`.

## Nota

- La base maestra del sistema es Supabase Postgres.
