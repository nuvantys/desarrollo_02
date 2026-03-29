# Dashboard Contifico

Dashboard estatico en `HTML + CSS + JS` alimentado desde snapshots JSON exportados desde PostgreSQL y complementado con una API local para la revision tecnica y la actualizacion bajo demanda.

## Archivos principales

- `export_dashboard_data.py`
- `local_dashboard_api.py`
- `dashboard/index.html`
- `dashboard/styles.css`
- `dashboard/app.js`
- `dashboard/charts.js`
- `dashboard/data/*.json`
- `dashboard/start_dashboard_server.ps1`
- `dashboard/start_dashboard_server.bat`
- `dashboard/start_dashboard_live_refresh.ps1`
- `dashboard/start_dashboard_live_refresh.bat`
- `dashboard/start_dashboard_full_refresh.ps1`
- `dashboard/start_dashboard_full_refresh.bat`

## Regenerar snapshot

PowerShell:

```powershell
$env:PGHOST = '127.0.0.1'
$env:PGPORT = '5432'
$env:PGUSER = 'postgres'
$env:PGPASSWORD = 'postgres'

python .\export_dashboard_data.py `
  --db-name contifico_backfill `
  --out-dir .\dashboard\data
```

## Levantar el dashboard local

Opcion rapida:

```powershell
.\dashboard\start_dashboard_server.ps1
```

Opcion por doble clic:

```text
dashboard\start_dashboard_server.bat
```

Modo vivo desde APIs en un clic:

```powershell
.\dashboard\start_dashboard_live_refresh.ps1
```

O por doble clic:

```text
dashboard\start_dashboard_live_refresh.bat
```

Modo completo desde APIs en un clic:

```powershell
.\dashboard\start_dashboard_full_refresh.ps1
```

O por doble clic:

```text
dashboard\start_dashboard_full_refresh.bat
```

El script levanta:

- servidor estatico en `http://127.0.0.1:8123`
- API tecnica local en `http://127.0.0.1:8130`
- si usas `start_dashboard_live_refresh.ps1`, tambien dispara el refresh rapido desde Contifico API
- si usas `start_dashboard_full_refresh.ps1`, dispara el refresh completo o backfill historico desde Contifico API

Opcion manual:

```powershell
cd .\dashboard
python -m http.server 8123
```

En otra consola:

```powershell
python .\local_dashboard_api.py --port 8130
```

Luego abrir:

```text
http://127.0.0.1:8123
```

## Que expone

- Tab `Revision tecnica` con estado del dataset, bitacora de corridas, volumen actualizado, salud relacional, watermarks y boton de refresh real
- La bitacora distingue entre `filas leidas en esta corrida` y `historico almacenado`, para no confundir optimizacion de lectura con perdida de datos.
- Tab `Vista analitica` con hero, comercial, clientes, inventario, logistica, tesoreria, contabilidad, calidad y tablas exportables
- Integracion nueva de `inventario/guia`, `banco/cuenta` y `banco/movimiento`, conectadas con `documentos`, `personas`, `bodegas`, `productos` y `cuentas_contables`
- Tres graficos nuevos sobre las capas integradas: trazabilidad de guias, carga logistica por bodega y flujo bancario mensual

## Notas

- El dashboard usa `fetch`, por eso debe abrirse con servidor HTTP y no por `file://`.
- Si abres `index.html` directo, la app muestra un aviso indicando que debes usar `http://127.0.0.1:8123`.
- La API tecnica local no expone secretos ni filas crudas de PostgreSQL; solo estado agregado del pipeline.
- El modo local conserva el ultimo snapshot estable; el modo vivo ejecuta un refresh rapido desde APIs y vuelve a publicar el snapshot en un solo clic.
- Para una reconstruccion historica completa, ejecuta manualmente `python .\\contifico_pg_backfill.py --mode backfill --db-name contifico_backfill`.
- La libreria de graficos se carga desde CDN de `ECharts`.
- La fuente de verdad del snapshot es `contifico_backfill`.
