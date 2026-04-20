@echo off
echo Creating UK Pokemon Monitor scheduled task...
echo This requires Administrator privileges.
echo.

schtasks /Delete /TN "UK Pokemon Monitor" /F >nul 2>&1

schtasks /Create ^
  /TN "UK Pokemon Monitor" ^
  /SC ONSTART ^
  /DELAY 0000:30 ^
  /TR "\"C:\WINDOWS\py.exe\" \"C:\pokemon-monitors\retailer-monitor\monitor_uk.py\"" ^
  /RU "HOME\Deiol" ^
  /RL HIGHEST ^
  /F

if %ERRORLEVEL% EQU 0 (
    echo.
    echo SUCCESS - Task created. Monitor will start 30 seconds after every boot.
) else (
    echo.
    echo FAILED - Make sure you right-clicked and chose "Run as administrator".
)
echo.
pause
