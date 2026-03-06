@echo off
setlocal

if not exist ".env" copy ".env.example" ".env" >nul
powershell -ExecutionPolicy Bypass -File ".\scripts\reset_local_db_env.ps1" -WipeData
if errorlevel 1 goto :fail

python ".\scripts\check_db_connection.py"
if errorlevel 1 goto :fail

python ".\scripts\init_db.py"
if errorlevel 1 goto :fail

python ".\scripts\reset_db.py"
if errorlevel 1 goto :fail

python ".\scripts\seed_data.py"
if errorlevel 1 goto :fail

python -m app.orchestration.pipeline run-full --source mock --posts 20 --events 250 --rebuild
if errorlevel 1 goto :fail

python ".\scripts\verify_orchestration.py" --simulate-failure --print-snapshot
if errorlevel 1 goto :fail

echo.
echo Recovery and verification completed successfully.
goto :eof

:fail
echo.
echo Command failed. Stopping batch run.
exit /b 1
