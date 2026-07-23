# Predictive Supply Chain Telemetry Pipeline

[![CI](https://github.com/NADEEMTHEBA8/supply-chain-telemetry-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/NADEEMTHEBA8/supply-chain-telemetry-pipeline/actions/workflows/ci.yml)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![Databricks](https://img.shields.io/badge/Databricks-Runtime%2015.x-FF3621)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-003366)
![AWS](https://img.shields.io/badge/AWS-S3%20%7C%20Kinesis-FF9900)
![dbt](https://img.shields.io/badge/dbt-1.8-orange)
![Terraform](https://img.shields.io/badge/Terraform-1.6-7B42BC)

Traditional ERP systems report on supply chain health hours or days after the fact. By the time a planner sees that a manufacturing line is at risk of stopping, the damage is done — production halts, safety stock is exhausted, and working capital is tied up in emergency procurement. This pipeline solves the **real-time supply chain intelligence** problem by ingesting factory machine telemetry and ERP inventory events at high throughput, processing them through a Medallion Lakehouse architecture on Databricks and AWS S3, and surfacing aggregated risk scores to Amazon Connect Decisions for automated supply planning.

---

## Visual Proof of Execution

<details>
<summary><b>View Pipeline Execution & Infrastructure Proof</b></summary>
<br>

**Figure 1: Databricks Auto Loader — Bronze Ingestion Running**
![Databricks Auto Loader](docs/assets/databricks_autoloader_running.png)

**Figure 2: AWS S3 — Delta Lake Bronze Layer (Real S3 Bucket)**
![AWS S3 Delta Lake](docs/assets/s3_delta_bronze_layer.png)

**Figure 3: Amazon Kinesis — Live Telemetry Stream Receiving Data**
![Kinesis Stream](docs/assets/kinesis_stream_incoming.png)

**Figure 4: Databricks SQL — Gold Supply Risk Table (Top 10 At-Risk Machines)**
![Gold Supply Risk](docs/assets/databricks_gold_supply_risk.png)

**Figure 5: dbt — All Supply Chain Models Green**
![dbt Models Green](docs/assets/dbt_models_success.png)

</details>

---

## Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                      │
│                                                                      │
│  Python TelemetryGenerator          PostgreSQL ERP (via Debezium)   │
│  (Factory IoT Sensors Simulation)   (AWS DMS in production)         │
└──────────────┬──────────────────────────────┬───────────────────────┘
               │                              │
               ▼                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                                   │
│                                                                      │
│         Amazon Kinesis Data Streams (te-machine-telemetry)          │
│         [Production: Amazon MSK — ~15,000 events/sec]               │
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Kinesis Firehose → S3
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    COMPUTE LAYER (Databricks)                        │
│                                                                      │
│  Notebook 01: Auto Loader (cloudFiles) reads S3 → Bronze Delta      │
│  Notebook 02: PySpark quality gate & enrichment → Silver Delta      │
│  Notebook 03: Aggregation & risk scoring → Gold Delta               │
│                                                                      │
│  Orchestration: Databricks Workflows (multi-task job, 15min cadence)│
└──────────────────────────────┬──────────────────────────────────────┘
                               │ Delta Lake writes
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER (AWS S3)                            │
│                                                                      │
│  s3://te-supply-chain-telemetry-lake/                               │
│  ├── raw/machine-telemetry/          ← Raw JSON from Kinesis        │
│  ├── delta/bronze_machine_telemetry/ ← Bronze Delta table (ACID)    │
│  ├── delta/silver_telemetry/         ← Silver Delta table           │
│  ├── delta/gold_supply_risk/         ← Gold Delta table             │
│  └── checkpoints/                    ← Auto Loader state            │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    SERVING LAYER                                     │
│                                                                      │
│  Amazon Connect Decisions ← Gold Delta (CDM-aligned supply risk)    │
│  Power BI Dashboards      ← Databricks SQL Warehouse                │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Technology Stack

| Layer | Tool | Version | Role |
|---|---|---|---|
| Language | Python | 3.11 | Pipeline code |
| Streaming | Amazon Kinesis Data Streams | — | Real-time telemetry ingestion |
| Compute | Databricks (PySpark) | Runtime 15.x | Stream processing & transformation |
| Storage | AWS S3 + Delta Lake | 3.x | Medallion Lakehouse (Bronze/Silver/Gold) |
| Transformation | dbt (`dbt-databricks`) | 1.8 | SQL-based Silver/Gold models |
| CDC | Debezium | 2.5 | ERP change capture (AWS DMS in production) |
| Orchestration | Databricks Workflows | — | Multi-task job scheduling |
| Infrastructure | Terraform (AWS provider) | 1.6 / 5.x | S3, Kinesis, IAM provisioning |
| CI | GitHub Actions | — | Linting and type checking |

---

## Architectural Decisions

**Databricks Auto Loader over raw Kafka consumer.** The Bronze ingestion notebook uses `spark.readStream.format("cloudFiles")` (Databricks Auto Loader) rather than consuming directly from Kinesis. Auto Loader uses a RocksDB checkpoint store to track which S3 files have been processed, providing exactly-once semantics and fault tolerance. When the stream resumes after a Databricks cluster restart, it picks up from the precise offset without reprocessing or losing records — critical for supply chain data integrity.

**`trigger(availableNow=True)` over continuous streaming.** The Bronze ingest job uses `trigger(availableNow=True)` rather than a continuous stream. This processes all pending files and terminates, allowing Databricks Workflows to execute it on a scheduled cadence (15 minutes) while retaining the stateful exactly-once guarantees of a streaming job. This optimises compute cost — clusters run for minutes, not continuously, aligning with TE Connectivity's mandate to maximise financial returns from cloud infrastructure.

**Delta Lake on S3 over PostgreSQL warehouse.** The original fraud pipeline used PostgreSQL as a local warehouse stand-in. This pipeline writes all layers directly as Delta tables on S3. Delta Lake provides ACID compliance, time travel, and schema enforcement directly on object storage — eliminating the need for a separate database tier and making every layer (Bronze, Silver, Gold) directly queryable from Databricks SQL with no additional infrastructure.

**Composite risk score over raw anomaly counts.** The `gold_supply_risk` table computes a weighted composite risk score per machine per day. Raw anomaly counts are not directly actionable for Amazon Connect Decisions — a normalised score (0.0–1.0) maps cleanly to the supply planning AI's confidence thresholds, allowing planners to configure automated replenishment triggers at `risk_score > 0.7` without custom logic.

**Kinesis over MSK for portfolio validation.** Amazon MSK requires a minimum of 2 brokers ($100+/month). Amazon Kinesis Data Streams demonstrates the identical streaming engineering skills at $1/month on a single shard. Both services are AWS-native, both use partition keys for shard affinity, and both integrate with Kinesis Firehose for S3 delivery. The engineering patterns are transferable to MSK with a single configuration change.

---

## The Elevator Pitch

> *"Looking at TE Connectivity's recent strategic shift toward prioritizing financial returns and operational efficiency over pure product innovation, I realized my previous fraud detection project was structurally identical to a supply chain telemetry pipeline. I re-architected it to focus on supply-demand imbalances, which I know is a major priority for your logistics teams.*
>
> *The pipeline ingests simulated factory telemetry via Amazon Kinesis, processes it using Databricks Auto Loader and PySpark Structured Streaming, and lands Bronze, Silver, and Gold Delta tables directly on AWS S3. The Gold tables are specifically designed to feed Amazon Connect Decisions by mapping to the Canonical Data Model's site and inventory_level entities — preventing line stoppages and reducing the working capital tied up in excess safety stock. It takes the Medallion architecture I built previously and focuses the output entirely on bottom-line impact and operational ROI, which I understand is the core mandate for your data engineering team this year."*

---

## Installation and Demo Run

### Prerequisites
- Docker + Docker Compose
- Python 3.11
- Terraform 1.6+
- AWS account with Access Key + Secret Key
- Databricks Community Edition account (`community.cloud.databricks.com`)

### Step 1 — Provision AWS Infrastructure
```bash
cd infra/terraform-aws-freetier
terraform init
terraform apply
# Note the outputs: s3_bucket_name, kinesis_stream_name, databricks_access_key_id
```

### Step 2 — Mount S3 in Databricks
Run this in a Databricks notebook once:
```python
dbutils.fs.mount(
    source="s3a://<YOUR_S3_BUCKET_NAME>",
    mount_point="/mnt/te-supply-chain",
    extra_configs={
        "fs.s3a.access.key": "<DATABRICKS_ACCESS_KEY_ID>",
        "fs.s3a.secret.key": "<DATABRICKS_SECRET_ACCESS_KEY>"
    }
)
```

### Step 3 — Start Local Services & Seed Reference Data
```bash
cp .env.example .env
docker compose up -d postgres debezium
python -m ingestion.telemetry_generator.src.seed_reference
```

### Step 4 — Push Telemetry to Kinesis
```bash
export KINESIS_STREAM_NAME=<YOUR_KINESIS_STREAM_NAME>
export AWS_ACCESS_KEY_ID=<YOUR_KEY>
export AWS_SECRET_ACCESS_KEY=<YOUR_SECRET>
python -m ingestion.telemetry_generator.src.run --events 500
```

### Step 5 — Run Databricks Notebooks
Import `notebooks/01_bronze_autoloader.py`, `02_silver_structuring.py`, and `03_gold_supply_risk.py` into your Databricks workspace and run them in sequence.

### Step 6 — Tear Down
```bash
cd infra/terraform-aws-freetier
terraform destroy
```
Total demo run cost: **< $0.10**

---

## Data Model

### Machine Telemetry Event (Kinesis payload)
```json
{
  "machine_id": "MCH_0042",
  "plant_id": "PLANT_MX_01",
  "event_timestamp": "2026-07-23T14:32:01Z",
  "temperature_celsius": 97.4,
  "vibration_hz": 88.2,
  "pressure_bar": 3.2,
  "operational_status": "FAULT",
  "error_code": "E001_OVERHEAT",
  "rpm": 142.5,
  "power_consumption_kw": 18.3
}
```

### Gold Supply Risk (Amazon Connect Decisions CDM)
| Column | Type | Maps to CDM Entity |
|---|---|---|
| `machine_id` | string | `site.id` |
| `plant_id` | string | `site.geo_id` |
| `avg_temp_24h` | double | Anomaly signal |
| `fault_event_count_24h` | long | Replenishment trigger |
| `risk_score` | double | Planning confidence threshold |
