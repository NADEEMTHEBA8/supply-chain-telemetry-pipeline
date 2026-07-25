# Predictive Supply Chain Telemetry Pipeline

[![CI](https://github.com/NADEEMTHEBA8/supply-chain-telemetry-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/NADEEMTHEBA8/supply-chain-telemetry-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Databricks](https://img.shields.io/badge/Databricks-Runtime%2015.x-FF3621)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-003366)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Kinesis-FF9900)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Terraform](https://img.shields.io/badge/Terraform-1.6-7B42BC)

High-frequency event-driven telemetry pipeline engineered for real-time manufacturing risk analytics. Ingests simulated machine IoT sensor streams across global plant locations, processes payloads through a Medallion Lakehouse on Databricks and AWS S3, and computes composite machine risk scores to prevent unplanned production stoppages.

---

## Architectural Overview

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    TELEMETRY & CDC SOURCES                          │
│                                                                      │
│  Python TelemetryGenerator          PostgreSQL ERP (via Debezium)   │
│  (Factory Floor IoT Emitter)        (AWS DMS in Production)         │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                                   │
│                                                                      │
│    Amazon Kinesis Data Streams / Direct S3 Object Ingestion         │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    COMPUTE & MEDALLION LAKEHOUSE (Databricks)       │
│                                                                      │
│  01_bronze_autoloader.py  → Auto Loader (cloudFiles) → Bronze Delta │
│  02_silver_structuring.py → Quality boundary gate → Silver Delta   │
│  03_gold_supply_risk.py   → 24h rolling risk metric → Gold Delta   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER (AWS S3)                            │
│                                                                      │
│  s3://te-supply-chain-telemetry-lake/                               │
│  ├── raw/machine-telemetry/          ← Raw JSON sensor payloads     │
│  ├── delta/bronze_machine_telemetry/ ← Bronze Delta table           │
│  ├── delta/silver_telemetry/         ← Silver Delta table           │
│  └── delta/gold_supply_risk/         ← Gold Supply Risk metrics     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Technical Trade-off Analysis & Architecture Decisions

### 1. Direct S3 Object Ingestion vs. Continuous Managed Kafka (Amazon MSK)
- **Trade-off:** Operating a multi-broker Amazon MSK cluster incurs ~$100+/month in fixed baseline infrastructure costs.
- **Decision:** For development and validation environments, writing partitioned JSON batches directly to S3 combined with Databricks Auto Loader (`cloudFiles`) provides micro-batch ingestion guarantees with near-zero idle compute costs.
- **Production Parity:** Production deployments map directly to Amazon MSK / Kinesis Firehose using the exact same downstream Auto Loader PySpark notebook routines.

### 2. Physical Lakehouse Partitioning (`plant_id`, `event_date`)
- **Trade-off:** Unpartitioned Delta tables cause full-table file scans when querying specific manufacturing plants or date ranges.
- **Decision:** All Silver and Gold Delta tables are explicitly partitioned by `plant_id` and `event_date`. This enables file skipping in Databricks SQL queries, cutting query scan volume by up to 90%.

### 3. Explicit Schema Contracts over Schema Inference
- **Trade-off:** PySpark `inferSchema=True` causes full-file preliminary passes and runtime type ambiguity during schema drift.
- **Decision:** Ingestion routines strictly enforce explicit `StructType` and `StructField` definitions with `schemaEvolutionMode="rescue"` to isolate malformed payloads into `_rescued_data`.

---

## Technical Stack

- **Language:** Python 3.11 (Typed dataclass domain models, Pydantic settings)
- **Compute:** Databricks (PySpark, Structured Streaming, Delta Lake 3.x)
- **Storage:** AWS S3 (Bronze, Silver, Gold Delta Tables)
- **Infrastructure as Code:** Terraform 1.6+ (Modular `main.tf`, `variables.tf`, `outputs.tf`)
- **Testing & Quality:** Pytest (Unit tests with `boto3` mocking), Ruff (Linting)

---

## Local Development & Test Execution

### 1. Run Unit Test Suite
```bash
.venv/bin/pytest -v
```

### 2. Execute Code Style & Linting Check
```bash
.venv/bin/ruff check .
```

### 3. Deploy Infrastructure via Terraform
```bash
cd infra/terraform-aws-freetier
terraform init
terraform apply
```

### 4. Emit Synthetic Machine Sensor Data to S3
```bash
.venv/bin/python -m ingestion.telemetry_generator.src.run --events 500 --firehose
```
