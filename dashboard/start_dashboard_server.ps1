$ErrorActionPreference = "Stop"

$dashboardDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Split-Path -Parent $dashboardDir
$port = 8123
$apiPort = 8130
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
Write-Host "Levantando API tecnica en http://127.0.0.1:$apiPort" -ForegroundColor Cyan
Start-Process $python.Source -ArgumentList (Join-Path $rootDir "local_dashboard_api.py"), "--port", "$apiPort" -WorkingDirectory $rootDir
Start-Sleep -Seconds 2
Start-Process $url

Write-Host "Stack iniciado. Si necesitas detenerlo, cierra las ventanas de Python correspondientes." -ForegroundColor Green
