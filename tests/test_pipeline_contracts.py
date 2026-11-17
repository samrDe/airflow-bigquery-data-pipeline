from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def test_dag_contains_expected_pipeline_tasks():
    dag_text = (ROOT / "dags/airports_pipeline.py").read_text(encoding="utf-8")
    for task in (
        "download_and_validate_source",
        "upload_csv_to_gcs",
        "load_raw_bigquery",
        "dbt_build_and_test",
    ):
        assert task in dag_text

def test_dbt_models_and_quality_tests_exist():
    assert (ROOT / "dbt/models/staging/stg_airports.sql").exists()
    assert (ROOT / "dbt/models/marts/airport_country_summary.sql").exists()
    assert (ROOT / "dbt/tests/no_blank_identifiers.sql").exists()
    assert (ROOT / "dbt/tests/valid_coordinate_ranges.sql").exists()
    schema = (ROOT / "dbt/models/staging/stg_airports.yml").read_text()
    assert "not_null" in schema and "unique" in schema and "accepted_values" in schema
