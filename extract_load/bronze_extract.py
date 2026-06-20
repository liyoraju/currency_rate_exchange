import os
import io
import json
from google.cloud import bigquery


# service_account: lets us create credentials from a service-account JSON key (instead of default ADC)
from google.oauth2 import service_account
import requests


DATASET = "bronze"
CURRENCIES_URL = "https://api.frankfurter.dev/v2/currencies"
RATES_URL = "https://api.frankfurter.dev/v2/rates"


def _client() -> bigquery.Client:
    # os.environ is a dict-like object holding the process's environment variables
    # ["GOOGLE_CREDENTIALS"] raises KeyError if the variable is not set
    creds_path = os.environ["GOOGLE_CREDENTIALS"]
    # open() returns a file object; "with" ensures it is closed automatically
    # encoding="utf-8" tells Python to decode the file as UTF-8 text
    with open(creds_path, encoding="utf-8") as f:
        # json.load() reads a file object and parses it into a Python dict
        info = json.load(f)
    # from_service_account_info() takes the dict and builds a credentials object
    # This object handles token refresh and signing automatically
    credentials = service_account.Credentials.from_service_account_info(info)
    # bigquery.Client() needs a project and credentials; here we pass only
    # credentials and the project is inferred from the "project_id" field in the key
    return bigquery.Client(credentials=credentials)


def fetch_data(url: str) -> dict | list:
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        if len(resp.json()) == 0:
            raise ValueError("No data returned from API")
        return resp.json()
    except requests.exceptions.Timeout:
        print(f"Timeout fetching {url}")
        raise
    except requests.exceptions.ConnectionError:
        print(f"Connection error fetching {url}")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"HTTP {e.response.status_code} for {url}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        raise
    except json.JSONDecodeError:
        print(f"Invalid JSON response from {url}")
        raise


def load_to_bq(file: dict, table_id: str) -> None:
    # Get an authenticated BigQuery client
    client = _client()
    # f-string building the fully-qualified table reference: project.dataset.table
    # client.project is the GCP project ID from the service account key
    table_ref = f"{client.project}.{DATASET}.{table_id}"

    # read_text() opens the file, reads all bytes, decodes as UTF-8, and returns a string
    # json.loads() parses that string into a Python object (list of dicts in our case)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    # Guard: if the JSON was an empty list [], there's nothing to load

    # Conditional expression (ternary): if overwrite is True use TRUNCATE, else APPEND
    # WRITE_TRUNCATE: replaces all existing data in the table
    # WRITE_APPEND: adds rows to whatever is already in the table

    # LoadJobConfig holds all settings for a load job
    # source_format tells BigQuery what format the input data is in
    # NEWLINE_DELIMITED_JSON means one JSON object per line (NDJSON / JSONL)
    # write_disposition controls overwrite vs append behaviour
    # autodetect=True tells BigQuery to infer column names and types from the data

    # Generator expression: for each row dict, dump it to a JSON string
    # "\n".join() concatenates them with newline separators → NDJSON format
    ndjson = "\n".join(json.dumps(row) for row in file)
    # load_table_from_file expects a file-like object (anything with .read())
    # io.StringIO wraps the ndjson string so it looks like a file to the client
    # table_ref is the target table, job_config contains the settings
    job = client.load_table_from_file(
        io.StringIO(ndjson), table_ref, job_config=job_config
    )
    # job.result() blocks until the load job finishes
    # raises an exception if the job failed
    job.result()
    # len(rows) is the number of Python objects we loaded
    print(f"Loaded {len(file)} rows into {table_ref}")


def main():
    # currencies: overwrite=True → WRITE_TRUNCATE (replace all rows)
    # rates:      overwrite=False → WRITE_APPEND (add rows to existing data)
    currencies_file = fetch_data(CURRENCIES_URL)
    rates_file = fetch_data(RATES_URL)

    load_to_bq(rates_file, "raw_rates")
    load_to_bq(currencies_file, "raw_currencies")


if __name__ == "__main__":
    main()
