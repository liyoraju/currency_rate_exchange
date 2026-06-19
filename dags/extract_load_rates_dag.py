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
        load_to_bq(rates_data, "rates")

    rates_info = extract_rates()
    load_rates(rates_info)


etl_rates()
