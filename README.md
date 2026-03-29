# Dashboard Analitico Contifico

Proyecto de extraccion, normalizacion, refresh cloud hacia Supabase Postgres y dashboard analitico estatico para explorar datos de Contifico sin depender de localhost.

## Incluye

- Extractor API y exportacion tabular
- Backfill historico a Supabase Postgres con relaciones y reporte final
- Exportador de snapshots JSON para analitica web
- Dashboard en `HTML`, `CSS` y `JavaScript`
- Funciones cloud y workflow para actualizar Supabase desde la pagina sin localhost

## Estructura

- `contifico_extractor.py`
- `contifico_pg_backfill.py`
- `export_dashboard_data.py`
- `supabase/functions/`
- `.github/workflows/contifico-cloud-refresh.yml`
- `dashboard/`
- `final_report.md`
- `README_contifico_extractor.md`
- `README_contifico_pg_backfill.md`
- `README_dashboard_contifico.md`

## Fuente de datos

La fuente de verdad para el dashboard es Supabase Postgres. El frontend consume snapshots JSON publicados desde esa base en la nube y el refresh se dispara mediante funciones cloud y GitHub Actions.
