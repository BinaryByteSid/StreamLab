#!/usr/bin/env sh
# macOS / Linux: start the StreamLab virtual lab and open it in the browser.
set -e
cd "$(dirname "$0")"
docker info >/dev/null 2>&1 || { echo "Docker is not running. Start Docker first."; exit 1; }
docker compose up -d --build
echo "Waiting for http://localhost:8080 ..."
i=0
until curl -fs http://localhost:8080/api/health >/dev/null 2>&1; do
  i=$((i + 1)); [ "$i" -gt 90 ] && { echo "The lab did not start; see: docker compose logs"; exit 1; }
  sleep 2
done
echo "StreamLab is running: http://localhost:8080"
(command -v open >/dev/null && open http://localhost:8080) || (command -v xdg-open >/dev/null && xdg-open http://localhost:8080) || true
