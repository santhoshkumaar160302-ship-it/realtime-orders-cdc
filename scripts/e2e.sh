#!/usr/bin/env bash
# End-to-end test: start the stack, generate a fixed amount of traffic,
# then require reconciliation and quality checks to pass.
set -euo pipefail
cd "$(dirname "$0")/.."

EVENTS="${E2E_EVENTS:-1500}"

dump_logs() {
  echo "---- streamer logs ----"; docker compose logs --tail=200 streamer || true
  echo "---- connect logs ----";  docker compose logs --tail=100 connect  || true
}
trap 'dump_logs' ERR

echo ">> starting infrastructure and streaming job"
docker compose up -d --build postgres kafka connect register-connector streamer

echo ">> generating ${EVENTS} source transactions"
docker compose run --rm -e MAX_EVENTS="$EVENTS" -e EVENTS_PER_SEC=50 -e SEED=42 simulator

echo ">> reconciling (retries while the stream catches up)"
docker compose run --rm -e RECON_ATTEMPTS=12 -e RECON_WAIT_SECONDS=15 jobs python jobs/reconcile.py

echo ">> quality checks"
docker compose run --rm jobs python jobs/quality_checks.py

echo ">> gold layer"
docker compose run --rm jobs python jobs/build_gold.py

echo "E2E PASSED"
