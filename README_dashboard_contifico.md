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

El script levanta:

- servidor estatico en `http://127.0.0.1:8123`
- API tecnica local en `http://127.0.0.1:8130`

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
- Tab `Vista analitica` con hero, comercial, clientes, inventario, contabilidad, calidad y tablas exportables

## Notas

- El dashboard usa `fetch`, por eso debe abrirse con servidor HTTP y no por `file://`.
- Si abres `index.html` directo, la app muestra un aviso indicando que debes usar `http://127.0.0.1:8123`.
- La API tecnica local no expone secretos ni filas crudas de PostgreSQL; solo estado agregado del pipeline.
- La libreria de graficos se carga desde CDN de `ECharts`.
- La fuente de verdad del snapshot es `contifico_backfill`.
