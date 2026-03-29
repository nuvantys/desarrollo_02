param(
  [switch]$RunLiveRefresh,
  [ValidateSet("refresh", "backfill")]
  [string]$RefreshScope = "refresh"
)

$ErrorActionPreference = "Stop"

$dashboardDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Split-Path -Parent $dashboardDir
$port = 8123
$apiPort = 8130
$url = "http://127.0.0.1:$port"
$apiUrl = "http://127.0.0.1:$apiPort/api/technical"

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

if ($RunLiveRefresh) {
  Write-Host "Esperando disponibilidad de la API tecnica para disparar refresh desde Contifico API..." -ForegroundColor Yellow
  $ready = $false
  foreach ($attempt in 1..12) {
    try {
      Invoke-WebRequest -UseBasicParsing "$apiUrl/status" | Out-Null
      $ready = $true
      break
    } catch {
      Start-Sleep -Seconds 2
    }
  }
  if ($ready) {
    try {
      $body = @{ scope = $RefreshScope } | ConvertTo-Json -Compress
      Invoke-WebRequest -UseBasicParsing -Method Post "$apiUrl/refresh" -ContentType "application/json" -Body $body | Out-Null
      $scopeLabel = if ($RefreshScope -eq "backfill") { "completo" } else { "rapido" }
      Write-Host "Refresh $scopeLabel disparado desde APIs. El dashboard seguira mostrando progreso en Revision tecnica." -ForegroundColor Green
    } catch {
      Write-Host "No fue posible disparar el refresh automatico. Revisa CONTIFICO_AUTHORIZATION y el estado de la API tecnica." -ForegroundColor Red
    }
  } else {
    Write-Host "La API tecnica no estuvo lista a tiempo para lanzar el refresh automatico." -ForegroundColor Red
  }
}

Start-Process $url

Write-Host "Stack iniciado. Si necesitas detenerlo, cierra las ventanas de Python correspondientes." -ForegroundColor Green
