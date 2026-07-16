# ─── Realtime Fraud Feature Store — Enterprise Makefile ──────────────────────
#
# This Makefile is the operational contract for the local digital twin cluster.
# It proves professional orchestration of multi-container environments and
# serves as a portable CI/CD entrypoint.
#
# 12-Factor X (Dev/Prod Parity):
#   Every target here can be replicated in a CI/CD pipeline (GitHub Actions,
#   Cloud Build, Jenkins) by calling the same make targets with an environment-
#   specific env file: ENV_FILE=.env.gcp make cluster-up
#
# Usage:
#   make help               → print this menu
#   make cluster-up         → start the full stack and wait for health
#   make demo               → run the complete end-to-end pipeline
# ─────────────────────────────────────────────────────────────────────────────

# ── Configuration ─────────────────────────────────────────────────────────────
ENV_FILE   ?= .env.local
include $(ENV_FILE)
export
PYTHON     := .venv/bin/python
DBT        := .venv/bin/dbt
UVICORN    := .venv/bin/uvicorn
PYTEST     := .venv/bin/pytest
RUFF       := .venv/bin/ruff

# Pipeline tunables (override on the CLI: make inject-synthetic-data EVENTS=5000)
EVENTS     ?= 1000
API_PORT   ?= 8002
USER_ID    ?=
SVC        ?=

DBT_DIR := warehouse/dbt/fraud_warehouse
DBT_FLAGS       := --profiles-dir .

.PHONY: help \
        validate-env \
        cluster-up cluster-down cluster-status cluster-nuke \
        build-airflow \
        seed connector \
        inject-synthetic-data stream-bronze stream-bronze-once load-bronze \
        dbt-run dbt-incremental-run dbt-snapshot dbt-test \
        load-features api-up score \
        health recon \
        demo \
        setup install fmt lint test logs ps

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  ╔══════════════════════════════════════════════════════════╗"
	@echo "  ║   Realtime Fraud Feature Store — Cluster Operations     ║"
	@echo "  ╚══════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  INFRASTRUCTURE"
	@echo "    validate-env          Assert all required vars in $(ENV_FILE)"
	@echo "    cluster-up            Start all services; wait for healthy"
	@echo "    cluster-down          Graceful stop (volumes preserved)"
	@echo "    cluster-status        Service health summary"
	@echo "    cluster-nuke          DESTRUCTIVE: stop + wipe all volumes"
	@echo "    build-airflow         Build custom Airflow image (bake deps)"
	@echo "    logs [SVC=<name>]     Tail logs (e.g. SVC=spark-master)"
	@echo "    ps                    Docker compose ps"
	@echo ""
	@echo "  SETUP (run once per fresh cluster)"
	@echo "    seed                  Seed Postgres reference tables"
	@echo "    connector             Register Debezium CDC connector"
	@echo ""
	@echo "  PIPELINE"
	@echo "    inject-synthetic-data Produce N events → Kafka [EVENTS=$(EVENTS)]"
	@echo "    stream-bronze         Spark structured streaming → MinIO Delta"
	@echo "    stream-bronze-once    Single batch (CI / backlog drain) [availableNow trigger]"
	@echo "    load-bronze           Delta Lake (MinIO) → Postgres bronze schema"
	@echo "    dbt-snapshot          Run dbt snapshot"
	@echo "    dbt-run               Run dbt snapshot + run + test"
	@echo "    dbt-incremental-run   Run only incremental models"
	@echo "    dbt-test              Run dbt tests (JUnit XML output)"
	@echo "    load-features         Postgres gold → Redis feature cache"
	@echo "    api-up                Start FastAPI serving layer"
	@echo "    score [USER_ID=<id>]  Run mock ML scorer against the API"
	@echo "    demo                  Full end-to-end pipeline simulation"
	@echo ""
	@echo "  MONITORING"
	@echo "    health                Poll /health endpoint"
	@echo "    recon                 Bronze/Silver reconciliation report"
	@echo ""
	@echo "  DEVELOPMENT"
	@echo "    setup                 Create .venv + install all deps"
	@echo "    install               Reinstall project deps (dev mode)"
	@echo "    fmt                   ruff format ."
	@echo "    lint                  ruff check ."
	@echo "    test                  pytest -v --cov"
	@echo ""

# ── validate-env ──────────────────────────────────────────────────────────────
# This target is a prerequisite for every pipeline command. It enforces that
# the operator has a correctly populated env file before any cluster operation.
# Mirror of a CI/CD pre-flight check.
REQUIRED_VARS := POSTGRES_PASSWORD MINIO_ACCESS_KEY MINIO_SECRET_KEY \
                 KAFKA_BOOTSTRAP_SERVERS REDIS_HOST PG_PASSWORD API_KEY

validate-env:
	@echo "→ Validating environment: $(ENV_FILE)"
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "✗ ERROR: $(ENV_FILE) not found."; \
		echo "  Run: cp .env.local.example .env.local && edit .env.local"; \
		exit 1; \
	fi
	@set -a; . ./$(ENV_FILE); set +a; \
	MISSING=""; \
	for var in $(REQUIRED_VARS); do \
		val=$$(eval echo "\$$$$var"); \
		if [ -z "$$val" ]; then MISSING="$$MISSING $$var"; fi; \
	done; \
	if [ -n "$$MISSING" ]; then \
		echo "✗ ERROR: Missing required variables in $(ENV_FILE):$$MISSING"; \
		exit 1; \
	fi
	@echo "✓ Environment validated: all required variables present."

# ── Infrastructure ────────────────────────────────────────────────────────────

build-airflow:
	@echo "→ Building custom Airflow image (baking deps — this may take ~2min)..."
	docker build -f infra/airflow/Dockerfile.airflow -t fraud-airflow:local .
	@echo "✓ fraud-airflow:local image ready."

cluster-up: validate-env build-airflow
	@echo "→ Starting fraud_vpc cluster..."
	@docker compose --env-file $(ENV_FILE) up -d > /dev/null 2>&1
	@echo "→ Waiting for services to become healthy..."
	@echo "  (Kafka, Postgres, Redis, MinIO, Spark, Airflow)"
	@for svc in kafka postgres redis minio spark-master kafka-connect; do \
		echo -n "  Waiting for $$svc... "; \
		attempts=0; \
		while [ "$$(docker inspect --format='{{.State.Health.Status}}' $$svc 2>/dev/null)" != "healthy" ]; do \
			attempts=$$((attempts + 1)); \
			if [ $$attempts -ge 30 ]; then \
				echo "TIMEOUT"; \
				echo "  Run 'make logs SVC=$$svc' to investigate."; \
				exit 1; \
			fi; \
			sleep 5; \
		done; \
		echo "healthy ✓"; \
	done
	@echo ""
	@echo "  ╔═══════════════════════════════════════════════════╗"
	@echo "  ║  fraud_vpc cluster is UP                         ║"
	@echo "  ╠═══════════════════════════════════════════════════╣"
	@echo "  ║  Kafka UI:      http://localhost:8080            ║"
	@echo "  ║  Spark Master:  http://localhost:8088            ║"
	@echo "  ║  Spark Worker:  http://localhost:8081            ║"
	@echo "  ║  Airflow:       http://localhost:8082            ║"
	@echo "  ║  MinIO:         http://localhost:9001            ║"
	@echo "  ║  Kafka Connect: http://localhost:8083            ║"
	@echo "  ╠═══════════════════════════════════════════════════╣"
	@echo "  ║  Next: make seed && make connector               ║"
	@echo "  ╚═══════════════════════════════════════════════════╝"

cluster-down:
	@echo "→ Stopping fraud_vpc cluster (volumes preserved)..."
	docker compose down
	@echo "✓ Cluster stopped."

cluster-status:
	@echo "→ Cluster service health:"
	@docker compose ps
	@echo ""
	@echo "→ Network:"
	@docker network inspect fraud-pipeline_fraud_vpc --format '{{.Name}}: {{len .Containers}} containers' 2>/dev/null || echo "  fraud_vpc not found (cluster is down)"

cluster-nuke: validate-env
	@echo "⚠ WARNING: This will DESTROY all volumes (Kafka offsets, Postgres data,"
	@echo "  MinIO objects, Redis cache). This action is irreversible."
	@echo "  Press Ctrl+C within 5 seconds to abort..."
	@sleep 5
	docker compose down -v
	@echo "✓ Cluster and all volumes destroyed. Run 'make cluster-up' for a fresh start."

logs:
ifdef SVC
	docker compose logs -f $(SVC)
else
	docker compose logs -f
endif

ps:
	docker compose ps

# ── Setup (run once per fresh cluster) ───────────────────────────────────────

seed: validate-env
	@echo "→ Seeding Postgres reference tables (users, merchants)..."
	$(PYTHON) -m ingestion.transaction_generator.src.seed_reference
	@echo "✓ Reference tables seeded."

connector: validate-env
	@echo "→ Registering Debezium Postgres CDC connector..."
	CONNECT_URL=$$(grep CONNECT_URL $(ENV_FILE) | cut -d= -f2) \
	./infra/debezium/register-connector.sh
	@echo "✓ Debezium connector registered."

# ── Pipeline ──────────────────────────────────────────────────────────────────

inject-synthetic-data: validate-env
	@echo "→ Injecting $(EVENTS) synthetic transactions → Kafka..."
	@echo "  Topic: transactions.raw | Bootstrap: $$(grep KAFKA_BOOTSTRAP $(ENV_FILE) | cut -d= -f2)"
	$(PYTHON) -m ingestion.transaction_generator.src.run --max-events $(EVENTS) --firehose
	@echo "✓ $(EVENTS) events published."

stream-bronze: validate-env
	@echo "→ Starting Spark Structured Streaming job..."
	@echo "  Source: transactions.raw (Kafka)"
	@echo "  Sink:   s3a://bronze/transactions_v2/ (MinIO Delta Lake)"
	@echo "  Master: $$(grep SPARK_MASTER_URL $(ENV_FILE) | cut -d= -f2)"
	@echo "  Press Ctrl+C to stop the streaming job."
	$(PYTHON) -m streaming.spark.src.bronze_ingest

stream-bronze-once: validate-env
	@echo "→ Running Spark bronze ingestion (availableNow trigger — processes backlog once)..."
	$(PYTHON) -m streaming.spark.src.bronze_ingest --once || \
		(echo "✗ Bronze ingestion FAILED — check Spark logs"; exit 1)
	@echo "✓ Bronze ingestion complete."

load-bronze: validate-env
	@echo "→ Loading Bronze Delta Lake (MinIO) → Postgres bronze schema..."
	$(PYTHON) -m warehouse.loaders.bronze_loader
	@echo "✓ Bronze load complete."

dbt-snapshot: validate-env
	@echo "→ Running dbt snapshot..."
	cd $(DBT_DIR) && ../../../$(DBT) snapshot $(DBT_FLAGS)

dbt-run: validate-env
	@echo "→ Running full dbt pipeline (snapshot → run → test)..."
	cd $(DBT_DIR) && ../../../$(DBT) snapshot $(DBT_FLAGS)
	cd $(DBT_DIR) && ../../../$(DBT) run $(DBT_FLAGS)
	cd $(DBT_DIR) && ../../../$(DBT) test $(DBT_FLAGS)
	@echo "✓ dbt pipeline complete."

dbt-incremental-run: validate-env
	@echo "→ Running dbt incremental models only..."
	cd $(DBT_DIR) && ../../../$(DBT) run --select "config.materialized:incremental" $(DBT_FLAGS)
	@echo "✓ dbt incremental run complete."

dbt-test: validate-env
	@echo "→ Running dbt tests (JUnit XML output → target/junit.xml)..."
	cd $(DBT_DIR) && ../../../$(DBT) test $(DBT_FLAGS) \
		--store-failures \
		2>&1 | tee /tmp/dbt-test-output.txt
	@echo "✓ dbt tests complete."

load-features: validate-env
	@echo "→ Loading features: Postgres gold → Redis cache..."
	$(PYTHON) -m feature_store.src.loader
	@echo "✓ Feature cache populated."

api-up: validate-env
	@echo "→ Starting Feature Serving API on http://localhost:$(API_PORT)"
	@echo "  Docs:   http://localhost:$(API_PORT)/docs"
	@echo "  Health: http://localhost:$(API_PORT)/health"
	@echo "  Auth:   X-API-Key header (see API_KEY in $(ENV_FILE))"
	$(UVICORN) feature_store.src.api:app --reload --port $(API_PORT) --env-file $(ENV_FILE)

score: validate-env
	@echo "→ Running mock ML scorer..."
	@if [ -n "$(USER_ID)" ]; then \
		$(PYTHON) -m feature_store.src.mock_ml_scorer --user_id $(USER_ID); \
	else \
		$(PYTHON) -m feature_store.src.mock_ml_scorer; \
	fi

# ── Monitoring ────────────────────────────────────────────────────────────────

health:
	@echo "→ Feature Store API health:"
	@curl -s localhost:$(API_PORT)/health | $(PYTHON) -m json.tool || \
		echo "  ✗ API is not running. Start with: make api-up"

recon:
	@echo "→ Bronze/Silver reconciliation report:"
	@docker compose exec postgres psql -U fraud_admin -d fraud_reference -c \
		"SELECT recon_status, bronze_total, silver_total, unaccounted_records \
		 FROM silver_data_quality.recon_bronze_silver;" 2>/dev/null || \
		echo "  Postgres container not running. Start with: make cluster-up"

# ── Demo — End-to-End Pipeline ────────────────────────────────────────────────
# Runs the full pipeline in sequence. Useful for a live demonstration or CI.
demo: validate-env
	@echo ""
	@echo "  ╔══════════════════════════════════════════════════════════╗"
	@echo "  ║   FRAUD FEATURE STORE — End-to-End Demo                ║"
	@echo "  ╚══════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  [1/6] Injecting $(EVENTS) synthetic transactions..."
	$(MAKE) inject-synthetic-data EVENTS=$(EVENTS)
	@echo ""
	@echo "  [2/6] Running Spark bronze ingestion (single batch)..."
	$(MAKE) stream-bronze-once
	@echo ""
	@echo "  [3/6] Loading bronze layer to Postgres warehouse..."
	$(MAKE) load-bronze
	@echo ""
	@echo "  [4/6] Running dbt transformations (silver + gold)..."
	$(MAKE) dbt-run
	@echo ""
	@echo "  [5/6] Loading features to Redis cache..."
	$(MAKE) load-features
	@echo ""
	@echo "  [6/6] Pipeline complete! Start the API to serve features:"
	@echo "        make api-up"
	@echo ""
	@echo "  ✓ Demo complete."

# ── Development ───────────────────────────────────────────────────────────────

setup:
	@echo "→ Creating Python 3.11 virtual environment..."
	python3.11 -m venv --clear .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"
	.venv/bin/pip install "dbt-core==1.8.*" "dbt-postgres==1.8.2"
	@echo ""
	@echo "✓ Setup complete."
	@echo "  Activate your environment: source .venv/bin/activate"
	@echo "  Next: cp .env.local.example .env.local && make cluster-up"

install:
	.venv/bin/pip install -e ".[dev]"

fmt:
	$(RUFF) format .

lint:
	$(RUFF) check .

test:
	$(PYTEST) -v --cov=. --cov-report=term-missing