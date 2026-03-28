# Dashboard Analitico Contifico

Proyecto de extraccion, normalizacion, backfill historico en PostgreSQL y dashboard analitico estatico para explorar datos de Contifico.

## Incluye

- Extractor API y exportacion tabular
- Backfill historico a PostgreSQL con relaciones y reporte final
- Exportador de snapshots JSON para analitica web
- Dashboard en `HTML`, `CSS` y `JavaScript`
- Scripts de arranque rapido para levantar el dashboard en servidor local

## Estructura

- `contifico_extractor.py`
- `contifico_pg_backfill.py`
- `export_dashboard_data.py`
- `dashboard/`
- `final_report.md`
- `README_contifico_extractor.md`
- `README_contifico_pg_backfill.md`
- `README_dashboard_contifico.md`

## Arranque rapido del dashboard

```powershell
cd dashboard
python -m http.server 8123
```

Abrir luego:

```text
http://127.0.0.1:8123
```

Tambien puedes usar:

```powershell
.\dashboard\start_dashboard_server.ps1
```

## Fuente de datos

La fuente de verdad para el dashboard es la base PostgreSQL `contifico_backfill`. El frontend consume snapshots JSON generados desde esa base.
