#!/usr/bin/env bash
# ─── Supply Chain Telemetry Pipeline — Demo Run Script ───────────────────────
# Runs the full end-to-end pipeline once for demonstration.
# AWS infrastructure must be provisioned first: make tf-apply
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

EVENTS=${EVENTS:-500}
LOG_DIR=".demo_logs"
mkdir -p "$LOG_DIR"

echo ""
echo "  ╔══════════════════════════════════════════════════════════╗"
echo "  ║  Supply Chain Telemetry Pipeline — Demo Run             ║"
echo "  ║  AWS Kinesis + Databricks + Delta Lake on S3            ║"
echo "  ╚══════════════════════════════════════════════════════════╝"
echo ""

echo "[1/4] Seeding PostgreSQL ERP reference tables..."
PYTHONWARNINGS="ignore" .venv/bin/python \
  -m ingestion.telemetry_generator.src.seed_reference \
  > "$LOG_DIR/1_seed.log" 2>&1
echo "[+] Postgres seeded: machines, suppliers, inventory_levels"

echo ""
echo "[2/4] Emitting $EVENTS machine telemetry events → Amazon Kinesis..."
PYTHONWARNINGS="ignore" .venv/bin/python \
  -m ingestion.telemetry_generator.src.run \
  --events "$EVENTS" --firehose \
  > "$LOG_DIR/2_kinesis.log" 2>&1
echo "[+] $EVENTS events published to Kinesis stream: $KINESIS_STREAM_NAME"

echo ""
echo "[3/4] AWS S3 → Databricks Auto Loader..."
echo "  Open Databricks and run these notebooks in order:"
echo "  1. notebooks/01_bronze_autoloader.py"
echo "  2. notebooks/02_silver_structuring.py"
echo "  3. notebooks/03_gold_supply_risk.py"
echo ""
echo "  After running, take screenshots of:"
echo "  - Databricks notebook output (bronze row count)"
echo "  - AWS S3 console showing delta/ folder"
echo "  - Databricks SQL: SELECT * FROM gold_supply_risk ORDER BY risk_score DESC LIMIT 10"
echo "  - Amazon Kinesis console showing incoming records"

echo ""
echo "[4/4] When done, tear down AWS resources:"
echo "  make tf-destroy   (costs < \$0.10 total)"
echo ""
echo "✓ Demo pipeline complete."
