@echo off
chcp 65001 >nul
cd /d "%~dp0"
if exist "%~dp0ERA5选取.exe" (
  start "" "%~dp0ERA5选取.exe"
  exit /b 0
)
py -3 "%~dp0app.py"
if errorlevel 1 (
  echo 启动失败。
  pause
)
