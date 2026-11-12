"""
Currency Exchange Rates ETL Pipeline
====================================
Airflow DAG that:
1. Extracts currency exchange rates from Frankfurter API (Base: EUR -> Quotes: USD, GBP, JPY).
2. Transforms the raw exchange rate records into the standardized schema.
3. Loads the transformed records into MongoDB.
Uses Airflow Named XCom to transfer data between tasks.
"""

from datetime import datetime, timedelta, timezone
import logging
import requests
from pymongo import MongoClient

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Configuration Constants
API_URL = "https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY"
MONGO_URI = "mongodb://root:rootpassword@mongodb:27017/?authSource=admin"
DATABASE_NAME = "currency_db"
COLLECTION_NAME = "exchange_rates"

# Named XCom Keys
RAW_DATA_XCOM_KEY = "raw_currency_data"
TRANSFORMED_DATA_XCOM_KEY = "transformed_currency_data"


def extract_currency_rates(**kwargs):
    """
    Task 1: Extract Currency Exchange Rates
    Fetches daily currency exchange rates from Frankfurter API and
    pushes the raw response into a Named Airflow XCom.
    """
    ti = kwargs["ti"]
    logging.info(f"Extracting exchange rates from API: {API_URL}")

    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    raw_data = response.json()

    logging.info(f"Successfully extracted {len(raw_data)} rate records from Frankfurter API.")
    logging.info(f"Raw data: {raw_data}")

    # Push to Named XCom
    ti.xcom_push(key=RAW_DATA_XCOM_KEY, value=raw_data)
    logging.info(f"Pushed raw rate data to Named XCom key: '{RAW_DATA_XCOM_KEY}'")


def transform_exchange_rate_data(**kwargs):
    """
    Task 2: Transform Exchange Rate Data
    Pulls raw currency rates from Named XCom, maps them into the required
    standardized schema, and pushes the transformed data to Named XCom.
    Required Schema:
    {
      "base_currency": "EUR",
      "target_currency": "USD",
      "exchange_rate": 1.15,
      "rate_date": "2026-11-04",
      "source": "Frankfurter API",
      "retrieved_at": "2026-11-05T07:15:00"
    }
    """
    ti = kwargs["ti"]

    # Pull from Named XCom of Task 1
    raw_data = ti.xcom_pull(key=RAW_DATA_XCOM_KEY, task_ids="extract_currency_rates")
    if not raw_data:
        raise ValueError(f"No data received from Named XCom key '{RAW_DATA_XCOM_KEY}'")

    logging.info(f"Pulled {len(raw_data)} raw records from Named XCom for transformation.")

    retrieved_at_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed_records = []

    for item in raw_data:
        record = {
            "base_currency": str(item.get("base")),
            "target_currency": str(item.get("quote")),
            "exchange_rate": float(item.get("rate")),
            "rate_date": str(item.get("date")),
            "source": "Frankfurter API",
            "retrieved_at": retrieved_at_timestamp,
        }
        transformed_records.append(record)

    logging.info(f"Transformed {len(transformed_records)} records successfully.")
    for rec in transformed_records:
        logging.info(f"Transformed record: {rec}")

    # Push transformed data to Named XCom
    ti.xcom_push(key=TRANSFORMED_DATA_XCOM_KEY, value=transformed_records)
    logging.info(f"Pushed transformed records to Named XCom key: '{TRANSFORMED_DATA_XCOM_KEY}'")


def load_data_into_mongodb(**kwargs):
    """
    Task 3: Load Data into MongoDB
    Pulls transformed records from Named XCom and inserts them into MongoDB.
    """
    ti = kwargs["ti"]

    # Pull from Named XCom of Task 2
    transformed_records = ti.xcom_pull(
        key=TRANSFORMED_DATA_XCOM_KEY, task_ids="transform_exchange_rate_data"
    )
    if not transformed_records:
        raise ValueError(f"No data received from Named XCom key '{TRANSFORMED_DATA_XCOM_KEY}'")

    logging.info(
        f"Connecting to MongoDB at {MONGO_URI}, DB: {DATABASE_NAME}, Collection: {COLLECTION_NAME}"
    )
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    # Insert records into MongoDB
    insert_result = collection.insert_many(transformed_records)
    inserted_count = len(insert_result.inserted_ids)

    logging.info(f"Successfully inserted {inserted_count} records into MongoDB!")
    logging.info(f"Inserted Document IDs: {insert_result.inserted_ids}")

    # Verify insertion
    total_docs = collection.count_documents({})
    logging.info(f"Total documents currently in '{COLLECTION_NAME}' collection: {total_docs}")

    client.close()


# Define the Airflow DAG
with DAG(
    dag_id="currency_exchange_etl_pipeline",
    default_args=default_args,
    description="Airflow ETL pipeline integrating daily Frankfurter currency rates into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["mlops", "currency_exchange", "mongodb", "etl"],
) as dag:

    # 1. Extract Task
    extract_task = PythonOperator(
        task_id="extract_currency_rates",
        python_callable=extract_currency_rates,
    )

    # 2. Transform Task
    transform_task = PythonOperator(
        task_id="transform_exchange_rate_data",
        python_callable=transform_exchange_rate_data,
    )

    # 3. Load Task
    load_task = PythonOperator(
        task_id="load_data_into_mongodb",
        python_callable=load_data_into_mongodb,
    )

    # Task Execution Order (Extract -> Transform -> Load)
    extract_task >> transform_task >> load_task

