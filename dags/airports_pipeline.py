"""Airflow DAG: public airport CSV -> GCS -> BigQuery -> dbt quality tests."""
from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

SOURCE_URL = "https://raw.githubusercontent.com/datasets/airport-codes/master/data/airport-codes.csv"
LOCAL_FILE = "/tmp/airport-codes.csv"
EXPECTED_COLUMNS = {
    "ident", "type", "name", "elevation_ft", "continent", "iso_country",
    "iso_region", "municipality", "gps_code", "iata_code", "local_code",
    "coordinates"
}

def download_and_check_source() -> None:
    """Download source CSV and fail early if it is empty or its schema changed."""
    with urlopen(SOURCE_URL, timeout=60) as response:
        content = response.read()
    if not content:
        raise ValueError("Source dataset download was empty.")
    header = next(csv.reader(content.decode("utf-8-sig").splitlines()))
    missing = EXPECTED_COLUMNS - set(header)
    if missing:
        raise ValueError(f"Source schema is missing expected columns: {sorted(missing)}")
    Path(LOCAL_FILE).write_bytes(content)

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
BUCKET = os.environ["GCS_BUCKET"]
DATASET = os.environ.get("BQ_DATASET", "airflow_demo")

with DAG(
    dag_id="airports_to_bigquery_quality",
    description="Ingest public airport data into BigQuery and enforce dbt quality tests.",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 2, "retry_delay": timedelta(minutes=3)},
    tags=["ingestion", "bigquery", "dbt", "data-quality"],
) as dag:
    download = PythonOperator(
        task_id="download_and_validate_source",
        python_callable=download_and_check_source,
    )

    upload = LocalFilesystemToGCSOperator(
        task_id="upload_csv_to_gcs",
        src=LOCAL_FILE,
        dst="landing/airport-codes/airport-codes.csv",
        bucket=BUCKET,
        mime_type="text/csv",
    )

    load = GCSToBigQueryOperator(
        task_id="load_raw_bigquery",
        bucket=BUCKET,
        source_objects=["landing/airport-codes/airport-codes.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET}.raw_airports",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        field_delimiter=",",
        allow_quoted_newlines=True,
    )

    dbt_build = BashOperator(
        task_id="dbt_build_and_test",
        bash_command=(
            "dbt build --project-dir /opt/airflow/dbt "
            "--profiles-dir /opt/airflow/dbt --target dev"
        ),
        env={
            "GCP_PROJECT_ID": PROJECT_ID,
            "BQ_DATASET": DATASET,
            "GCP_REGION": os.environ.get("GCP_REGION", "US"),
            "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/secrets/gcp-key.json",
        },
    )

    download >> upload >> load >> dbt_build
