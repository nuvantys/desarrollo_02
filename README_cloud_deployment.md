# Despliegue cloud-only con login

El proyecto ya quedo preparado para operar sin `localhost` y con base maestra en Supabase. El siguiente paso es publicarlo con autenticacion real para que el dashboard consuma el snapshot privado desde la nube.

## Arquitectura final

1. El usuario inicia sesion con Supabase Auth.
2. El frontend pide cada asset del snapshot a `dashboard-snapshot`.
3. `dashboard-snapshot` valida la sesion y lee `app.snapshot_assets`.
4. El usuario autorizado puede disparar `contifico-refresh`.
5. Supabase Edge Functions despacha GitHub Actions.
6. GitHub Actions actualiza Supabase desde Contifico.
7. `export_dashboard_data.py` regenera `dashboard/data/*.json` y publica cada archivo en `app.snapshot_assets`.
8. El frontend vuelve a leer el snapshot privado actualizado.

## Secrets en GitHub

- `CONTIFICO_AUTHORIZATION`
- `SUPABASE_DB_URL`

## Secrets en Supabase Edge Functions

- `GITHUB_WORKFLOW_TOKEN`
- `GITHUB_OWNER=nuvantys`
- `GITHUB_REPO=desarrollo_02`
- `GITHUB_WORKFLOW_FILE=contifico-cloud-refresh.yml`
- `GITHUB_REF=main`

## Funciones a desplegar en Supabase

- `supabase/functions/dashboard-snapshot`
- `supabase/functions/contifico-refresh`
- `supabase/functions/contifico-refresh-status`

## Configuracion del frontend

`dashboard/config.js` debe contener:

```js
window.CONTIFICO_CONFIG = {
  snapshotBase: "./data",
  snapshotApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/dashboard-snapshot",
  refreshApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh",
  refreshStatusUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh-status",
  supabaseUrl: "https://anaeoorbwnpstuievcwr.supabase.co",
  supabaseAnonKey: "<SUPABASE_ANON_KEY>",
};
```

## Recomendacion de hosting

El repo ya quedo listo para publicar `dashboard/` por GitHub Pages con el workflow `.github/workflows/deploy-dashboard-pages.yml`.

Lectura correcta del escenario actual:

- si `desarrollo_02` pasa a publico, GitHub Pages es la via mas directa
- si el repo sigue privado dentro de una organizacion `Free`, GitHub puede no permitir Pages y entonces la salida correcta es Vercel o Netlify

La aplicacion ya no depende de `localhost`; solo necesita un hosting estatico para servir `dashboard/`.

## Pendiente operativo

- El usuario `admin@nuvantys.com` ya puede autenticarse en Supabase Auth.
- `dashboard/config.js` ya quedo con la `supabaseAnonKey` real.
- Si se usa GitHub Pages, revisa el primer run de `deploy-dashboard-pages` en la pestaña `Actions`.
