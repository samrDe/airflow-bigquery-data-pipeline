# Airflow + BigQuery Data Pipeline with dbt Data Quality

A portfolio-ready data engineering project demonstrating orchestration, cloud ingestion, SQL transformation, and automated data-quality checks.

**Stack:** Apache Airflow · Google Cloud Storage · BigQuery · dbt Core · Docker Compose · GitHub Actions

> This is an implementation scaffold. It does not contain GCP credentials and has not been run against a Google Cloud project. Configure GCP and complete a successful DAG run before claiming cloud execution.

## Architecture

```text
Public airport CSV
      |
      v
Airflow DAG
  1. Download + validate source columns
      |
  2. Upload CSV to Google Cloud Storage
      |
  3. Load CSV into BigQuery raw table
      |
  4. Run dbt build
       |-- staging model
       |-- country/type summary mart
       |-- schema tests
       |-- custom SQL tests
```

## Repository structure

```text
.
├── .github/workflows/ci.yml
├── dags/airports_pipeline.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/stg_airports.sql
│   │   ├── staging/stg_airports.yml
│   │   └── marts/airport_country_summary.sql
│   └── tests/
├── tests/
├── Dockerfile
├── docker-compose.yml
├── .env.example
└── requirements-test.txt
```

## 1. Google Cloud setup

You need a Google Cloud project with billing enabled.

1. Create or select a GCP project.
2. Enable BigQuery API and Cloud Storage API.
3. Create a Cloud Storage bucket.
4. Create a BigQuery dataset, for example `airflow_demo`.
5. Create a service account with permissions to write to the bucket and create/load tables in the dataset. Use least-privilege IAM roles; do not use Owner/Editor for routine work.
6. For local learning, create a JSON key and save it outside Git, for example `C:\gcp-keys\airflow-demo.json`.

Never commit service-account keys, passwords, access tokens, or `.env` files.

## 2. Configure local environment

Copy `.env.example` to `.env`, then replace the placeholder values:

```dotenv
GCP_PROJECT_ID=your-gcp-project-id
GCP_REGION=US
GCS_BUCKET=your-unique-bucket-name
BQ_DATASET=airflow_demo
```

Create a `secrets` folder and place your key at `secrets/gcp-key.json`. The Compose file mounts it read-only into the containers. This local-key approach is for learning only; use Workload Identity or a managed identity in production.

## 3. Run Airflow locally

Requirements: Docker Desktop with Docker Compose.

```bash
docker compose up airflow-init
docker compose up -d
```

Open `http://localhost:8080`.

Local demo login:
- Username: `airflow`
- Password: `airflow`

Enable and trigger `airports_to_bigquery_quality`. The DAG downloads the public CSV, uploads it to GCS, loads the raw BigQuery table, then runs dbt transformations and tests.

View logs:

```bash
docker compose logs -f airflow-scheduler
```

Stop:

```bash
docker compose down
```

## 4. Data quality checks

The dbt project includes:
- `ident`: not null and unique.
- `airport_type`: not null and accepted values.
- `iso_country`: not null.
- Custom SQL test rejects blank identifiers.
- Custom SQL test checks longitude and latitude ranges.

The staging model standardises column names and safely casts coordinates. The mart aggregates airport counts by country and airport type.

## 5. GitHub Actions

CI compiles Python, runs lightweight repository tests, and parses the dbt project. CI does not connect to GCP and does not prove that a cloud DAG run succeeds. Validate cloud integration separately after configuring credentials and resources.

## 6. Evidence of a successful run

After a successful DAG run, verify these BigQuery tables:
- `raw_airports`
- `stg_airports`
- `airport_country_summary`

Capture genuine screenshots of:
- Green Airflow DAG graph.
- BigQuery raw and transformed tables.
- dbt build output with passing tests.

Add screenshots to `docs/screenshots/` if you want to showcase execution evidence.

## Resume-safe description

Use this after completing a successful cloud run:

**Orchestrated Data Pipeline with Airflow, BigQuery & dbt** — Built an Airflow DAG to ingest a public airport dataset into Google Cloud Storage and BigQuery, transform raw data with dbt, and enforce schema and SQL data-quality tests.

Until the GCP pipeline has been executed, describe it as an implementation project rather than a completed cloud deployment.

## Limitations

- Learning/portfolio project, not a production data platform.
- Source CSV may change; production systems should pin source snapshots or store ingestion-date partitions.
- Demo replaces the raw BigQuery table on each run. Production pipelines should consider incremental loads, partitioning, idempotency, schema evolution, alerting and lineage.
- Use Secret Manager/Workload Identity and least-privilege IAM in production.
