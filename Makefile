# ─── Predictive Supply Chain Telemetry Pipeline — Makefile ───────────────────
#
# Operational contract for the local development cluster and AWS demo run.
# Proves professional orchestration of multi-container environments.
#
# 12-Factor X (Dev/Prod Parity):
#   Every target here can be replicated in CI/CD (GitHub Actions, Cloud Build)
#   by calling the same make targets with an environment-specific env file:
#   ENV_FILE=.env.gcp make cluster-up
#
# Usage:
#   make help               → print this menu
#   make cluster-up         → start all local services
#   make demo               → full end-to-end pipeline simulation
# ─────────────────────────────────────────────────────────────────────────────

# ── Configuration ─────────────────────────────────────────────────────────────
ENV_FILE   ?= .env.local
include $(ENV_FILE)
export
PYTHON     := .venv/bin/python
DBT        := .venv/bin/dbt
PYTEST     := .venv/bin/pytest
RUFF       := .venv/bin/ruff

# Pipeline tunables (override on CLI: make emit-telemetry EVENTS=5000)
EVENTS     ?= 500
SVC        ?=

DBT_DIR    := warehouse/dbt/supply_chain_warehouse
DBT_FLAGS  := --profiles-dir .

.PHONY: help \
        validate-env \
        cluster-up cluster-down cluster-status cluster-nuke \
        build-airflow \
        seed connector \
        emit-telemetry \
        dbt-run dbt-incremental-run dbt-snapshot dbt-test \
        tf-init tf-apply tf-destroy \
        health recon \
        demo \
        setup install fmt lint test logs ps

# ── Help ──────────────────────────────────────────────────────────────────────
help:
	@echo ""
	@echo "  ╔══════════════════════════════════════════════════════════╗"
	@echo "  ║  Predictive Supply Chain Telemetry Pipeline             ║"
	@echo "  ║  AWS Kinesis + Databricks + Delta Lake on S3            ║"
	@echo "  ╚══════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  INFRASTRUCTURE (Local Docker)"
	@echo "    validate-env          Assert all required vars in $(ENV_FILE)"
	@echo "    cluster-up            Start all local services; wait for healthy"
	@echo "    cluster-down          Graceful stop (volumes preserved)"
	@echo "    cluster-status        Service health summary"
	@echo "    cluster-nuke          DESTRUCTIVE: stop + wipe all volumes"
	@echo "    logs [SVC=<name>]     Tail logs (e.g. SVC=postgres)"
	@echo "    ps                    Docker compose ps"
	@echo ""
	@echo "  AWS INFRASTRUCTURE (Terraform)"
	@echo "    tf-init               terraform init"
	@echo "    tf-apply              Provision S3 bucket + Kinesis stream + IAM"
	@echo "    tf-destroy            DESTRUCTIVE: tear down all AWS resources"
	@echo ""
	@echo "  SETUP (run once per fresh cluster)"
	@echo "    seed                  Seed Postgres ERP reference tables"
	@echo "    connector             Register Debezium CDC connector"
	@echo ""
	@echo "  PIPELINE"
	@echo "    emit-telemetry        Push N machine events → Kinesis [EVENTS=$(EVENTS)]"
	@echo "    dbt-snapshot          Run dbt snapshot (SCD Type 2)"
	@echo "    dbt-run               Run dbt snapshot + run + test"
	@echo "    dbt-incremental-run   Run only incremental models"
	@echo "    dbt-test              Run dbt tests (JUnit XML output)"
	@echo ""
	@echo "  MONITORING"
	@echo "    health                Check Postgres + Debezium connector health"
	@echo "    recon                 Bronze/Silver reconciliation report"
	@echo ""
	@echo "  DEMO (full end-to-end AWS + Databricks run)"
	@echo "    demo                  Provision AWS → emit telemetry → Databricks"
	@echo ""
	@echo "  DEVELOPMENT"
	@echo "    setup                 Create .venv + install all deps"
	@echo "    install               Reinstall project deps (dev mode)"
	@echo "    fmt                   ruff format ."
	@echo "    lint                  ruff check ."
	@echo "    test                  pytest -v --cov"
	@echo ""

# ── validate-env ──────────────────────────────────────────────────────────────
REQUIRED_VARS := POSTGRES_PASSWORD KINESIS_STREAM_NAME AWS_ACCESS_KEY_ID \
                 AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION PG_PASSWORD

validate-env:
	@echo "→ Validating environment: $(ENV_FILE)"
	@if [ ! -f "$(ENV_FILE)" ]; then \
		echo "✗ ERROR: $(ENV_FILE) not found."; \
		echo "  Run: cp .env.example .env.local && edit .env.local"; \
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

# ── AWS Infrastructure (Terraform) ───────────────────────────────────────────

tf-init:
	@echo "→ Initialising Terraform..."
	cd infra/terraform-aws-freetier && terraform init
	@echo "✓ Terraform initialised."

tf-apply: tf-init
	@echo "→ Provisioning AWS infrastructure (S3 + Kinesis + IAM)..."
	@echo "  This creates real AWS resources. Estimated cost: ~$1/month."
	cd infra/terraform-aws-freetier && terraform apply
	@echo "✓ AWS infrastructure provisioned."
	@echo "  Note the outputs above — you need s3_bucket_name and kinesis_stream_name."

tf-destroy:
	@echo "⚠ WARNING: This will DESTROY all AWS resources (S3 bucket, Kinesis stream)."
	@echo "  Press Ctrl+C within 5 seconds to abort..."
	@sleep 5
	cd infra/terraform-aws-freetier && terraform destroy
	@echo "✓ All AWS resources destroyed. Cost stopped."

# ── Infrastructure (Local Docker) ────────────────────────────────────────────

build-airflow:
	@echo "→ Building Airflow image..."
	docker build -f infra/airflow/Dockerfile.airflow -t supply-chain-airflow:local .
	@echo "✓ supply-chain-airflow:local image ready."

cluster-up: validate-env build-airflow
	@echo "→ Starting supply_chain_vpc cluster..."
	@docker compose --env-file $(ENV_FILE) up -d > /dev/null 2>&1
	@echo "→ Waiting for services to become healthy..."
	@echo "  (Postgres, Debezium/Kafka Connect)"
	@for svc in postgres kafka-connect; do \
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
	@echo "  ║  supply_chain_vpc cluster is UP                  ║"
	@echo "  ╠═══════════════════════════════════════════════════╣"
	@echo "  ║  Postgres:      localhost:5432                   ║"
	@echo "  ║  Kafka Connect: http://localhost:8083            ║"
	@echo "  ╠═══════════════════════════════════════════════════╣"
	@echo "  ║  Next: make seed && make connector               ║"
	@echo "  ╚═══════════════════════════════════════════════════╝"

cluster-down:
	@echo "→ Stopping supply_chain_vpc cluster (volumes preserved)..."
	docker compose down
	@echo "✓ Cluster stopped."

cluster-status:
	@echo "→ Cluster service health:"
	@docker compose ps

cluster-nuke: validate-env
	@echo "⚠ WARNING: This will DESTROY all volumes (Postgres data, Debezium offsets)."
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
	@echo "→ Seeding Postgres ERP reference tables (machines, suppliers, inventory_levels)..."
	$(PYTHON) -m ingestion.telemetry_generator.src.seed_reference
	@echo "✓ Reference tables seeded."

connector: validate-env
	@echo "→ Registering Debezium Postgres CDC connector..."
	CONNECT_URL=$$(grep CONNECT_URL $(ENV_FILE) | cut -d= -f2) \
	./infra/debezium/register-connector.sh
	@echo "✓ Debezium connector registered. CDC events now flowing to Kafka."

# ── Pipeline ──────────────────────────────────────────────────────────────────

emit-telemetry: validate-env
	@echo "→ Emitting $(EVENTS) machine telemetry events → Amazon Kinesis..."
	@echo "  Stream: $$(grep KINESIS_STREAM_NAME $(ENV_FILE) | cut -d= -f2)"
	@echo "  Region: $$(grep AWS_DEFAULT_REGION $(ENV_FILE) | cut -d= -f2)"
	$(PYTHON) -m ingestion.telemetry_generator.src.run --events $(EVENTS) --firehose
	@echo "✓ $(EVENTS) telemetry events published to Kinesis."
	@echo "  Next: run notebooks 01 → 02 → 03 in Databricks."

dbt-snapshot: validate-env
	@echo "→ Running dbt snapshot (SCD Type 2 on machines + inventory)..."
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

# ── Monitoring ────────────────────────────────────────────────────────────────

health:
	@echo "→ Postgres health:"
	@docker compose exec postgres pg_isready -U supply_chain_admin || \
		echo "  ✗ Postgres not running. Start with: make cluster-up"
	@echo "→ Debezium connector status:"
	@curl -s http://localhost:8083/connectors/reference-postgres-source/status 2>/dev/null | \
		$(PYTHON) -m json.tool || echo "  ✗ Kafka Connect not running."

recon:
	@echo "→ Bronze/Silver reconciliation report:"
	@docker compose exec postgres psql -U supply_chain_admin -d supply_chain_db -c \
		"SELECT * FROM data_quality.recon_bronze_silver;" 2>/dev/null || \
		echo "  Postgres container not running. Start with: make cluster-up"

# ── Demo — Full End-to-End Pipeline ──────────────────────────────────────────
demo: validate-env
	@echo ""
	@echo "  ╔══════════════════════════════════════════════════════════╗"
	@echo "  ║  Supply Chain Telemetry Pipeline — Demo Run             ║"
	@echo "  ║  AWS Kinesis + Databricks + Delta Lake on S3            ║"
	@echo "  ╚══════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "  [1/4] Provisioning AWS infrastructure (S3 + Kinesis)..."
	$(MAKE) tf-apply
	@echo ""
	@echo "  [2/4] Seeding local Postgres ERP reference tables..."
	$(MAKE) seed
	@echo ""
	@echo "  [3/4] Emitting $(EVENTS) telemetry events → Amazon Kinesis..."
	$(MAKE) emit-telemetry EVENTS=$(EVENTS)
	@echo ""
	@echo "  [4/4] Pipeline running! Open Databricks and run:"
	@echo "        notebooks/01_bronze_autoloader.py"
	@echo "        notebooks/02_silver_structuring.py"
	@echo "        notebooks/03_gold_supply_risk.py"
	@echo ""
	@echo "  After taking screenshots, tear down AWS resources with:"
	@echo "        make tf-destroy"
	@echo ""
	@echo "  ✓ Demo pipeline complete."

# ── Development ───────────────────────────────────────────────────────────────

setup:
	@echo "→ Creating Python 3.11 virtual environment..."
	python3.11 -m venv --clear .venv
	.venv/bin/pip install --upgrade pip
	.venv/bin/pip install -e ".[dev]"
	.venv/bin/pip install "dbt-core==1.8.*" "dbt-databricks==1.8.*" "boto3>=1.34"
	@echo ""
	@echo "✓ Setup complete."
	@echo "  Activate: source .venv/bin/activate"
	@echo "  Next: cp .env.example .env.local && edit .env.local && make tf-apply"

install:
	.venv/bin/pip install -e ".[dev]"

fmt:
	$(RUFF) format .

lint:
	$(RUFF) check .

test:
	$(PYTEST) -v --cov=. --cov-report=term-missing