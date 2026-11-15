@echo off
title StreamLab - hybrid mode (laptop + AWS S3 / CloudWatch)
cd /d "%~dp0"
if not exist ".env.aws" (
  echo  .env.aws is missing. Create it from the CloudShell output:
  echo    terraform output -raw env_file
  echo  See deploy\aws-hybrid\README.md
  pause
  exit /b 1
)
docker info >nul 2>&1
if errorlevel 1 (
  echo  Docker is not running. Start Docker Desktop first.
  pause
  exit /b 1
)
echo  Starting StreamLab with Amazon S3 + CloudWatch ...
docker compose --env-file .env.aws -f docker-compose.yml -f docker-compose.hybrid.yml up -d --build
if errorlevel 1 (
  echo  Something went wrong. See: docker compose logs
  pause
  exit /b 1
)
powershell -NoProfile -Command "for($i=0;$i -lt 90;$i++){ try { if((Invoke-WebRequest -UseBasicParsing http://localhost:8080/api/health -TimeoutSec 2).StatusCode -eq 200){break} } catch {}; Start-Sleep 2 }"
start "" http://localhost:8080/#/cloud
echo  Lab: http://localhost:8080   (the AWS links are on the "AWS deployment" page)
pause
