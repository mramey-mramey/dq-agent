@echo off
setlocal

set VENV=%~dp0venv\Scripts

:: Load BACKEND_URL from .env if present
for /f "usebackq tokens=1,* delims==" %%A in ("%~dp0.env") do (
    if /i "%%A"=="BACKEND_URL" set BACKEND_URL=%%B
)
if not defined BACKEND_URL set BACKEND_URL=http://localhost:8001

echo Starting DQ Agent backend on %BACKEND_URL%...
start "DQ Agent Backend" cmd /k "%VENV%\uvicorn.exe backend.main:app --reload --host 0.0.0.0 --port 8001"

echo Waiting for backend to initialize...
timeout /t 3 /nobreak >nul

echo Starting Streamlit frontend...
start "DQ Agent Frontend" cmd /k "set BACKEND_URL=%BACKEND_URL% && %VENV%\streamlit.exe run frontend\app.py"

echo.
echo Both services started. Close the two terminal windows to stop them.
endlocal
