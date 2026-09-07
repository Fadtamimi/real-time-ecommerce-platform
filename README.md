# Real-Time E-commerce Data Platform

An end-to-end learning project that turns raw e-commerce files into reliable
analytics data. It demonstrates local batch processing and a Databricks
medallion architecture: Bronze, Silver, and Gold.

## Current milestone: Databricks Bronze and Silver complete

```text
Raw CSV / JSON files -> Bronze Delta tables -> Silver cleaned tables -> Gold analytics
```

Databricks Free Edition verification:

| Layer | Tables | Verified rows |
| --- | --- | --- |
| Bronze | customers, products, events | 5, 5, 6 |
| Silver | customers, products, events | 5, 5, 6 |
| Gold | sales by country, category, and product | notebook source ready |

## Repository layout

```text
data/raw/          Small source-like CSV and JSON files
docs/data-model.md  Entity definitions and relationship notes
docs/databricks-runbook.md  Databricks workspace and milestone notes
notebooks/          Databricks Bronze, Silver, and Gold notebook sources
tests/              Repeatable data integrity checks
```

## Databricks workflow

The Databricks notebook sources under `notebooks/` use these objects:

- Schema: `workspace.ecommerce`
- Raw-data path: `/Volumes/workspace/ecommerce/raw/raw/`
- Bronze tables: `bronze_customers`, `bronze_products`, `bronze_events`
- Silver tables: `silver_customers`, `silver_products`, `silver_events`

Run the notebooks in order:

1. `01_bronze_ingestion.py`
2. `02_silver_transformations.py`
3. `03_gold_analytics.py`

The notebooks use Delta Lake and `mode("overwrite")` so this learning demo can
be rerun. A production pipeline would use incremental ingestion and `MERGE`.

## Run the Phase 1 check

From the repository root:

```bash
python -m unittest discover -s tests -v
```

The test reads the raw files and checks key integrity and basic data quality.

## Generate sample data automatically

The generator uses Python's standard library and a fixed seed, so the same
command produces reproducible data. It writes to `data/generated/` and does not
overwrite the small hand-written seed files in `data/raw/`.

```bash
python src/generators/generate_raw_data.py
```

You can change the volume and seed when practicing:

```bash
python src/generators/generate_raw_data.py --customers 100 --products 25 --events 1000 --seed 7
```

## Docker Compose and Kafka

`compose.yaml` describes the local Kafka service as configuration. Docker
Compose reads that file, creates a project network, downloads the Kafka image,
and starts the Kafka container.

- An **image** is the packaged Kafka blueprint.
- A **container** is the running Kafka instance created from that image.
- A **port mapping** (`9092:9092`) lets local Python code reach Kafka.
- The Compose network lets services in the same project find one another.

Start Kafka:

```bash
docker compose up -d
```

Check its status:

```bash
docker compose ps
```

Stop Kafka when you are finished:

```bash
docker compose down
```

## Silver transformation

The Silver job reads valid Bronze JSONL and enriches each event by joining it
with the customer and product reference files. It also normalizes timestamps,
converts numeric values, and calculates `total_amount` for purchases.

Run it with:

```bash
python src/processing/transform_to_silver.py
```

Silver output is written to `data/processed/silver_events.jsonl`. Unlike Bronze,
Silver is shaped for reliable downstream analysis rather than preserving the
source structure exactly.

## Gold summaries

The Gold job aggregates Silver purchase events into business-ready summaries:

- Revenue by country
- Revenue by category
- Revenue by product

Run it with:

```bash
python src/processing/transform_to_gold.py
```

The summaries are written to `data/processed/gold/`. Gold is designed for
analytics and dashboards, so consumers can query totals instead of processing
every raw event themselves.

## Structured logging and monitoring

Pipeline jobs emit one JSON log record per important operation. The logs include
the operation name, status, record count, duration, Kafka topic, partition, and
offset where applicable. This makes logs searchable by Docker, Airflow, or a
cloud logging system.

Useful monitoring events include:

- `kafka_publish`: event acknowledged by Kafka
- `bronze_write`: event stored after validation
- `event_rejected`: invalid event sent to dead letter
- `event_duplicate`: duplicate event skipped
- `silver_transform`: Silver row count and duration
- `gold_transform`: Gold input count and duration

## First PySpark job

PySpark lets the same transformation style run across a distributed Spark
cluster. `src/processing/spark_gold.py` reads Silver JSONL with an explicit
schema, filters purchase events, groups by category, and calculates totals.
For learning, it runs locally with all available CPU cores.

Set `JAVA_HOME` to your Java 17 installation, then run:

```powershell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.20.101-hotspot"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
python src/processing/spark_gold.py
```

Spark transformations such as `filter`, `groupBy`, and `agg` are lazy: they
build an execution plan without immediately processing all rows. Actions such
as `show`, `count`, and `write` trigger that plan. This is one reason Spark can
optimize a pipeline before executing it.

## Delta Lake

Delta Lake stores Spark tables with data files plus a transaction log. The log
allows reliable table versions and safer concurrent or incremental writes. The
local demo writes Silver data to a Delta table and reads it back:

```powershell
python src/processing/delta_demo.py
```

The table is written under `data/processed/delta/`, which is local practice
storage and is excluded from Git.

## Terraform cloud blueprint

The `terraform/` directory defines, but does not apply, the planned GCP layer:

- Cloud Storage bucket for lake data
- BigQuery dataset for Gold analytics

Copy `terraform/terraform.tfvars.example` to `terraform/terraform.tfvars`,
replace the placeholders, and run these commands only after authenticating to
GCP:

```powershell
terraform -chdir=terraform init
terraform -chdir=terraform plan
```

`plan` previews changes. Nothing is created until `terraform apply` is run.
Cloud resources can incur charges, so review the plan and billing first.

## Airflow orchestration

`dags/ecommerce_pipeline.py` defines a daily DAG with this dependency chain:

```text
source validation -> Silver transformation -> Gold transformation
```

Airflow owns scheduling, retries, and task dependencies. The existing Python
modules still own the actual data processing. The DAG syntax is validated, but
Airflow execution will begin after the Airflow Docker runtime is added.

The local Airflow stack is defined in `compose.airflow.yaml` and uses Postgres
for Airflow metadata. Set the values from `.env.example` in your terminal, then
start it with:

```powershell
$env:AIRFLOW_DB_PASSWORD = "local_airflow_db_password"
$env:AIRFLOW_SECRET_KEY = "local_airflow_secret_key_change_me"
$env:AIRFLOW_ADMIN_USERNAME = "admin"
$env:AIRFLOW_ADMIN_PASSWORD = "admin"
docker compose -f compose.airflow.yaml up -d
```

Open the UI at `http://localhost:8080`, then stop the stack with:

```powershell
docker compose -f compose.airflow.yaml down
```

The DAG was manually tested successfully with all three tasks completing in
order. These local credentials are for learning only and must be replaced by
secret management in a real deployment.

## Idempotent Bronze ingestion

The consumer uses `event_id` as the idempotency key. Before writing a valid
event, it loads IDs already present in the Bronze JSONL file. A repeated Kafka
delivery is acknowledged and skipped instead of being written twice.

In the live demo, Kafka delivered 12 messages because the same 6 events were
published twice. Bronze stored 6 unique records and skipped 6 duplicates.

## Schema evolution

Events now support an optional `currency` field. Existing events without that
field remain valid and receive the default `USD`; newer events can provide a
supported code such as `SAR`. This additive change avoids breaking older
producers while allowing Silver to carry currency information downstream.

The rule is:

```text
Missing currency → default USD
Supported currency → accept
Unknown currency → reject to dead letter
```

### Recommended local environment

Run Spark and Delta inside Ubuntu on WSL2. Docker Desktop continues to run
Kafka on Windows. From PowerShell:

```powershell
wsl -d Ubuntu -- bash -lc "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64; export SPARK_LOCAL_IP=127.0.0.1; cd /mnt/c/Users/Admin/Desktop/real-time-ecommerce-platform; /home/admin/ecommerce-venv/bin/python src/processing/delta_demo.py"
```

This Linux environment avoids Windows-only Hadoop native-library issues and
matches the Linux environment commonly used by Spark production systems.

### Delta time travel

`src/processing/delta_time_travel.py` demonstrates an initial write, an append,
and a historical read with `versionAsOf`. Delta keeps the old table version in
its transaction history instead of losing it when new data is appended.

```powershell
wsl -d Ubuntu -- bash -lc "export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64; export SPARK_LOCAL_IP=127.0.0.1; cd /mnt/c/Users/Admin/Desktop/real-time-ecommerce-platform; /home/admin/ecommerce-venv/bin/python src/processing/delta_time_travel.py"
```
