@echo off
setlocal

REM One-shot local recovery + start script for social-attribution-engine.
REM Destructive: resets Docker DB volume data.

cd /d "%~dp0\.."

set "DOCKER_BIN=C:\Program Files\Docker\Docker\resources\bin"
if exist "%DOCKER_BIN%\docker.exe" (
  set "PATH=%DOCKER_BIN%;%PATH%"
)

echo [1/9] Ensuring .env exists...
if not exist ".env" (
  copy /Y ".env.example" ".env" >nul
)

echo [2/9] Resetting Docker Postgres (destructive)...
docker compose down -v
if errorlevel 1 goto :fail
docker compose up -d
if errorlevel 1 goto :fail

echo [3/9] Waiting for DB container...
set /a DB_READY=0
for /L %%i in (1,1,30) do (
  python .\scripts\check_db_connection.py >nul 2>&1
  if not errorlevel 1 (
    set /a DB_READY=1
    goto :db_ready
  )
  timeout /t 2 /nobreak >nul
)

:db_ready
if %DB_READY%==0 (
  echo DB did not become ready in time.
  goto :fail
)

set "PYTHONPATH=."

echo [4/9] Checking DB connection...
python .\scripts\check_db_connection.py
if errorlevel 1 goto :fail

echo [5/9] Initializing schema...
python .\scripts\init_db.py
if errorlevel 1 goto :fail

echo [6/9] Resetting DB schema...
python .\scripts\reset_db.py
if errorlevel 1 goto :fail

echo [7/9] Seeding data...
python .\scripts\seed_data.py
if errorlevel 1 goto :fail

echo [8/9] Installing frontend dependencies...
npm --prefix frontend install --include=dev
if errorlevel 1 goto :fail

echo [9/9] Restarting backend/frontend services...
for /f "tokens=5" %%p in ('netstat -ano ^| findstr /r /c:":8000 .*LISTENING"') do taskkill /PID %%p /F >nul 2>&1
for /f "tokens=5" %%p in ('netstat -ano ^| findstr /r /c:":5173 .*LISTENING"') do taskkill /PID %%p /F >nul 2>&1

start "" cmd /c "cd /d %cd% && python -m uvicorn app.orchestration.api:app --host 127.0.0.1 --port 8000"
start "" cmd /c "cd /d %cd%\frontend && npm run dev -- --host 127.0.0.1 --port 5173"

echo.
echo Done.
echo Backend:  http://127.0.0.1:8000/health
echo Frontend: http://127.0.0.1:5173
exit /b 0

:fail
echo.
echo Failed with exit code %errorlevel%.
exit /b %errorlevel%
