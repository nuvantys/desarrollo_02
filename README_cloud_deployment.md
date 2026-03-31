# Despliegue cloud-only

El proyecto opera con base maestra en Supabase y sitio publicado en la nube. No requiere runtime local para servir el dashboard ni para disparar el refresh.

## Arquitectura final

1. El usuario entra al sitio publicado con login simple de frontend.
2. El frontend lee el snapshot web publicado y consulta funciones cloud para estado y refresh.
3. `contifico-refresh` despacha GitHub Actions.
4. GitHub Actions actualiza Supabase desde Contifico.
5. `export_dashboard_data.py` regenera `dashboard/data/*.json`.
6. GitHub Pages publica el snapshot renovado y el frontend lo vuelve a consumir.

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

- `supabase/functions/contifico-refresh`
- `supabase/functions/contifico-refresh-status`
- `supabase/functions/dashboard-bootstrap`
- `supabase/functions/dashboard-snapshot`

## Configuracion del frontend

`dashboard/config.js` debe contener:

```js
window.CONTIFICO_CONFIG = {
  snapshotBase: "./data",
  bootstrapApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/dashboard-bootstrap",
  snapshotApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/dashboard-snapshot",
  refreshApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh",
  refreshStatusUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh-status",
  supabaseUrl: "",
  supabaseAnonKey: "",
  simpleLogin: {
    email: "admin@nuvantys.com",
    password: "Nuvant@1410",
  },
};
```

## Recomendacion de hosting

El repo ya quedo listo para publicar `dashboard/` por GitHub Pages con el workflow `.github/workflows/deploy-dashboard-pages.yml`.

Lectura correcta del escenario actual:

- si `desarrollo_02` es publico, GitHub Pages es la via mas directa
- si el repo no puede usar Pages, la salida correcta es Vercel o Netlify

La aplicacion solo necesita hosting estatico para servir `dashboard/`.

## Pendiente operativo

- el sitio publicado debe servir la carpeta `dashboard/`
- las funciones desplegadas en Supabase deben coincidir con el codigo del repo
- si se usa GitHub Pages, revisa el workflow `deploy-dashboard-pages` en `Actions`
