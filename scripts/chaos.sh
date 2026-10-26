#!/usr/bin/env bash
# Failure-recovery drill: while traffic is flowing, hard-kill the Spark job
# and restart Kafka Connect, then prove the lake still matches the source
# exactly. This is the "exactly-once results" claim, tested.
set -euo pipefail
cd "$(dirname "$0")/.."

echo ">> making sure traffic is flowing"
docker compose start simulator
sleep 30

echo ">> SIGKILL the streaming job mid-batch"
docker compose kill -s SIGKILL streamer
sleep 30   # source keeps changing while the consumer is down

echo ">> restart Kafka Connect (Debezium resumes from its replication slot)"
docker compose restart connect
sleep 20

echo ">> bring the streaming job back (resumes from its checkpoint)"
docker compose start streamer
sleep 45

echo ">> stop traffic and reconcile"
docker compose stop simulator
docker compose run --rm -e RECON_ATTEMPTS=10 jobs python jobs/reconcile.py
docker compose run --rm jobs python jobs/quality_checks.py
echo "CHAOS DRILL PASSED: no lost, duplicated or stale rows after failures"
