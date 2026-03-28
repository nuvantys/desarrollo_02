# Dashboard Contifico

Dashboard estatico en `HTML + CSS + JS` alimentado desde snapshots JSON exportados desde PostgreSQL.

## Archivos principales

- `export_dashboard_data.py`
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

Opcion manual:

```powershell
cd .\dashboard
python -m http.server 8123
```

Luego abrir:

```text
http://127.0.0.1:8123
```

## Que expone

- Hero analitico con KPIs, cobertura y alertas
- Comercial: documentos, mix, top clientes y top productos
- Clientes y productos: concentracion, roles, categorias y marcas
- Inventario: flujo, bodegas, rotacion y costo por categoria
- Contabilidad: actividad mensual, cuentas, centros de costo y balance
- Calidad de datos: diferencias fuente vs core, placeholders, nulos permitidos y salud relacional

## Notas

- El dashboard usa `fetch`, por eso debe abrirse con servidor HTTP y no por `file://`.
- Si abres `index.html` directo, la app muestra un aviso indicando que debes usar `http://127.0.0.1:8123`.
- La libreria de graficos se carga desde CDN de `ECharts`.
- La fuente de verdad del snapshot es `contifico_backfill`.
