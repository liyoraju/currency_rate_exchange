import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime

from airflow.sdk import dag, task
from extract_load.bronze_extract import (
    load_to_bq,
    fetch_data,
    CURRENCIES_URL,
)


@dag(schedule="@daily", start_date=datetime(2026, 6, 20), catchup=False)
def etl_currencies():

    @task.python
    def extract_currencies():
        return fetch_data(CURRENCIES_URL)

    @task.python
    def load_currencies(currencies_data):
        load_to_bq(currencies_data, "raw_currencies")

    @task.bash
    def transform():
        return (
            "cd /opt/airflow/dbt_currency && dbt run --select silver_currencies && dbt test --select silver_currencies"
            "dbt run --select gold_usd_inr && dbt test --select gold_usd_inr"
        )

    rates_info = extract_currencies()
    load_task = load_currencies(rates_info)
    transform_task = transform()

    load_task >> transform_task


etl_currencies()
