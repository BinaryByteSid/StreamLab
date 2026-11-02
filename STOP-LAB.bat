@echo off
title StreamLab - stopping
cd /d "%~dp0"
echo Stopping the StreamLab containers (your data is kept for next time)...
docker compose down
echo.
echo Stopped. To also delete all lab data, run:  docker compose down -v
pause
