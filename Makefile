JOB = docker compose run --rm jobs python

.PHONY: up down clean ps logs connector pause resume reconcile quality gold maintenance stats chaos e2e test lint

up:            ## build and start the whole stack
	docker compose up -d --build

down:          ## stop containers, keep data
	docker compose down

clean:         ## stop and delete all data (Postgres, Kafka, lake, checkpoints)
	docker compose down -v

ps:
	docker compose ps

logs:          ## follow the streaming job
	docker compose logs -f streamer

connector:     ## Debezium connector status
	curl -s localhost:8083/connectors/shop-postgres-cdc/status | python3 -m json.tool

pause:         ## stop generating traffic (do this before reconciling)
	docker compose stop simulator

resume:
	docker compose start simulator

reconcile:     ## row-by-row Postgres vs lake comparison
	$(JOB) jobs/reconcile.py

quality:       ## data quality checks on silver
	$(JOB) jobs/quality_checks.py

gold:          ## build gold tables + SCD2 customer history
	$(JOB) jobs/build_gold.py

maintenance:   ## pause the streamer, compact files + expire snapshots, resume
	docker compose stop streamer
	$(JOB) jobs/maintenance.py; status=$$?; docker compose start streamer; exit $$status

stats:         ## throughput / latency numbers + time travel demo
	$(JOB) jobs/pipeline_stats.py

chaos:         ## kill components mid-stream, then prove nothing was lost
	./scripts/chaos.sh

e2e:           ## full end-to-end test (same as CI)
	./scripts/e2e.sh

test:
	pytest -q

lint:
	ruff check .
