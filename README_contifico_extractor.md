# Contifico Extractor

Extractor standalone para construir una fuente de datos viva de Contifico en CSV UTF-8, con exportacion opcional a Excel.

## Requisitos

- Python 3.11+
- Variable de entorno `CONTIFICO_AUTHORIZATION`
- Dependencias:
  - `requests`
  - `openpyxl` para `dataset.xlsx`

## Variables de entorno

PowerShell:

```powershell
$env:CONTIFICO_AUTHORIZATION = 'tu_token_aqui'
```

## Backfill historico

```powershell
python .\contifico_extractor.py `
  --mode backfill `
  --output-dir .\dataset_contifico `
  --save-raw `
  --export-xlsx
```

## Incremental por ventana explicita

```powershell
python .\contifico_extractor.py `
  --mode incremental `
  --output-dir .\dataset_contifico `
  --from-date 2026-03-27 `
  --to-date 2026-03-28 `
  --overlap-days 2 `
  --save-raw `
  --export-xlsx
```

## Incremental desde watermark

Si `watermarks.csv` ya existe, puedes omitir `--from-date` y dejar solo `--to-date`, o dejar ambos vacios para tomar `today` como corte superior.

```powershell
python .\contifico_extractor.py `
  --mode incremental `
  --output-dir .\dataset_contifico `
  --to-date 2026-03-28 `
  --export-xlsx
```

## Salidas

El extractor genera estas tablas:

- `personas.csv`
- `productos.csv`
- `movimientos.csv`
- `movimiento_detalles.csv`
- `documentos.csv`
- `documento_detalles.csv`
- `documento_cobros.csv`
- `tickets_documentos.csv`
- `tickets_detalles.csv`
- `tickets_items.csv`
- `asientos.csv`
- `asiento_detalles.csv`
- `periodos.csv`
- `categorias.csv`
- `bodegas.csv`
- `unidades.csv`
- `marcas.csv`
- `cuentas_contables.csv`
- `centros_costo.csv`
- `extract_runs.csv`
- `watermarks.csv`

Si usas `--save-raw`, tambien guarda los payloads JSON bajo `raw/<run_id>/`.

## Seguridad

- El token nunca se lee desde argumentos ni se guarda en CSV.
- No copies el token dentro de scripts, notebooks ni archivos del dataset.
- Si el token ya fue compartido fuera de un canal seguro, debe rotarse.
