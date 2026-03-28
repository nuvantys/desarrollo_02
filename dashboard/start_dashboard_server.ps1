$ErrorActionPreference = "Stop"

$dashboardDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$port = 8123
$url = "http://127.0.0.1:$port"

Set-Location -LiteralPath $dashboardDir

$python = Get-Command python -ErrorAction SilentlyContinue
if (-not $python) {
  Write-Host "Python no esta disponible en PATH. Instala Python o ejecuta el servidor manualmente." -ForegroundColor Red
  Write-Host "Comando esperado: python -m http.server $port"
  exit 1
}

Write-Host "Levantando dashboard en $url" -ForegroundColor Cyan
Start-Process $python.Source -ArgumentList "-m", "http.server", "$port" -WorkingDirectory $dashboardDir
Start-Sleep -Seconds 2
Start-Process $url

Write-Host "Servidor iniciado. Si necesitas detenerlo, cierra la ventana de Python correspondiente." -ForegroundColor Green
