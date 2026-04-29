@echo off
setlocal
chcp 65001 >nul

cd /d "%~dp0"

if not exist "runtime-state" mkdir "runtime-state"

set "DASHBOARD_URL=http://127.0.0.1:8765/"
set "SIM_OUT=runtime-state\okx-sim-oneclick.out.log"
set "SIM_ERR=runtime-state\okx-sim-oneclick.err.log"
set "DASH_OUT=runtime-state\dashboard.out.log"
set "DASH_ERR=runtime-state\dashboard.err.log"

echo [%date% %time%] Starting OKX simulated trading loop...
start "OKX Simulated Trading" /min cmd /c "python main.py --profile okx-sim-active --live --okx-simulated-trading --auto-loop --max-scan-cycles 0 --poll-interval-sec 60 --runtime-state-dir runtime-state --output-format json >> %SIM_OUT% 2>> %SIM_ERR%"

echo [%date% %time%] Starting dashboard at %DASHBOARD_URL% ...
start "Trading Dashboard" /min cmd /c "python scripts\dashboard.py --host 127.0.0.1 --port 8765 --panel OKX-Sim=runtime-state --panel Binance-Futures-Testnet=runtime-state-binance-futures-testnet >> %DASH_OUT% 2>> %DASH_ERR%"

timeout /t 3 /nobreak >nul
start "" "%DASHBOARD_URL%"

echo.
echo OKX simulated trading and dashboard have been started.
echo Dashboard: %DASHBOARD_URL%
echo.
echo Logs:
echo   %SIM_OUT%
echo   %SIM_ERR%
echo   %DASH_OUT%
echo   %DASH_ERR%
echo.
pause
