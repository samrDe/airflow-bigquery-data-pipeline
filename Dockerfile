FROM apache/airflow:2.10.5-python3.11
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-google==10.22.0 \
    dbt-bigquery==1.9.1
