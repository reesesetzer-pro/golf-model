@echo off
cd /d "C:\Golf Model"

:: Only run during tournament round hours (8 AM - 9 PM ET)
:: Task Scheduler fires every 10 min all day — this gate keeps it quiet overnight
for /f "tokens=1 delims=:" %%h in ("%TIME%") do set HOUR=%%h
set HOUR=%HOUR: =%

if %HOUR% LSS 8 (
    echo Skipping sync — outside round hours ^(%TIME%^)
    exit /b 0
)
if %HOUR% GEQ 21 (
    echo Skipping sync — outside round hours ^(%TIME%^)
    exit /b 0
)

echo Running live sync at %TIME%...
py golf_sync.py --mode live
