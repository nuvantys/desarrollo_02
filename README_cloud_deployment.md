# Despliegue cloud-only

Este proyecto ya quedó preparado para operar sin `localhost`. Para activarlo de punta a punta faltan solo pasos de infraestructura:

## 1. Publicar el dashboard

Publica `dashboard/` en GitHub Pages, Netlify o Vercel.

## 2. Crear secrets en GitHub

En el repositorio agrega:

- `CONTIFICO_AUTHORIZATION`
- `SUPABASE_DB_URL`

## 3. Desplegar funciones en Supabase

Publica:

- `supabase/functions/contifico-refresh`
- `supabase/functions/contifico-refresh-status`

## 4. Crear secrets en Supabase Edge Functions

- `GITHUB_WORKFLOW_TOKEN`
- `GITHUB_OWNER=nuvantys`
- `GITHUB_REPO=desarrollo_02`
- `GITHUB_WORKFLOW_FILE=contifico-cloud-refresh.yml`
- `GITHUB_REF=main`

## 5. Verificar `dashboard/config.js`

Debe apuntar al proyecto correcto:

```js
window.CONTIFICO_CONFIG = {
  snapshotBase: "./data",
  refreshApiUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh",
  refreshStatusUrl: "https://anaeoorbwnpstuievcwr.supabase.co/functions/v1/contifico-refresh-status",
};
```

## Resultado

Cuando pulses `Refresh rapido` o `Refresh completo`:

1. la pagina llamará a Supabase
2. Supabase disparará GitHub Actions
3. GitHub Actions actualizará Supabase desde Contifico
4. GitHub Actions regenerará `dashboard/data/*.json`
5. el dashboard publicado quedará actualizado sin tocar `localhost`
