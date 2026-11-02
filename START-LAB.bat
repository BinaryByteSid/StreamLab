@echo off
title StreamLab - starting the virtual lab
cd /d "%~dp0"
echo.
echo  ============================================================
echo    StreamLab - Streaming Window Metrics Aggregator Virtual Lab
echo  ============================================================
echo.
docker info >nul 2>&1
if errorlevel 1 (
  echo  Docker is not running. Please start Docker Desktop, wait until it says
  echo  "Engine running", then double-click this file again.
  echo.
  pause
  exit /b 1
)
echo  Starting 23 containers. The first time this downloads and builds
echo  images and can take 5-10 minutes. Later starts take about a minute.
echo.
docker compose up -d --build
if errorlevel 1 (
  echo.
  echo  Something went wrong. Run "docker compose logs" to see why.
  pause
  exit /b 1
)
echo.
echo  Waiting for the lab to answer on http://localhost:8080 ...
powershell -NoProfile -Command "$ok=$false; for($i=0;$i -lt 90;$i++){ try { $r=Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/health -TimeoutSec 2; if($r.StatusCode -eq 200){$ok=$true;break} } catch {}; Start-Sleep 2 }; if(-not $ok){ exit 1 }"
start "" http://localhost:8080
echo.
echo  The lab is open in your browser: http://localhost:8080
echo  To stop it later, double-click STOP-LAB.bat
echo.
pause
