$ErrorActionPreference = "Stop"

$dashboardDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $dashboardDir

& (Join-Path $dashboardDir "start_dashboard_server.ps1") -RunLiveRefresh -RefreshScope refresh
