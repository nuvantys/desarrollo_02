@echo off
setlocal
cd /d "%~dp0"
start "" powershell -ExecutionPolicy Bypass -File "%~dp0start_dashboard_live_refresh.ps1"
