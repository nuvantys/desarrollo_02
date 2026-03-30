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

Publica `dashboard/` en Vercel o Netlify. GitHub Pages solo conviene si la configuracion del snapshot y del login queda ya cerrada y no necesitas headers o reglas adicionales.

## Pendiente operativo

Hace falta crear al menos un usuario en Supabase Auth y cargar la `supabaseAnonKey` en `dashboard/config.js` o inyectarla desde el hosting.
