@echo off
setlocal
set "SCRIPT=%~dp0harness.ps1"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT%" %*
exit /b %ERRORLEVEL%
