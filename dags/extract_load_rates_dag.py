import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from datetime import datetime

from airflow.sdk import dag, task
from extract_load.bronze_extract import load_to_bq, fetch_data, RATES_URL


@dag(schedule="@hourly", start_date=datetime(2026, 6, 20), catchup=False)
def etl_rates():

    @task.python
    def extract_rates():
        return fetch_data(RATES_URL)

    @task.python
    def load_rates(rates_data):
        load_to_bq(rates_data, "raw_rates")

    @task.bash
    def transform():
        return "cd /opt/airflow/dbt_currency && dbt run --select silver_rates && dbt test --select silver_rates && dbt run --select gold_daily_rates && dbt test --select gold_daily_rates"

    rates_info = extract_rates()
    load_task = load_rates(rates_info)
    transform_task = transform()

    load_task >> transform_task


etl_rates()
