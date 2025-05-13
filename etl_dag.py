from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pandas as pd
from faker import Faker
import random
from google.cloud import storage
import io

PROJECT_ID = "comp-840-383000"
DATASET_ID = "international_studebts"
BUCKET_NAME = "international_students"
FILE_URI = f"gs://{BUCKET_NAME}/International_Education_Costs.csv"

RAW_TABLE = "raw_university_data"
INDIA_TABLE = "indian_students"
UK_TABLE = "uk_students"
USA_TABLE = "usa_students"
AUS_TABLE = "australia_students"

default_args = {
    "start_date": datetime(2024, 11, 1),
    "catchup": False,
}

with DAG(
    dag_id="etl_gcs_to_bq_filtered_tables",
    schedule_interval=None,
    default_args=default_args,
    description="ETL from GCS to BigQuery with filtered country-specific tables",
    tags=["gcs", "bigquery", "etl"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # 1. Load raw data from GCS into a staging BigQuery table
    load_raw_data = BigQueryInsertJobOperator(
        task_id="load_raw_data",
        configuration={
            "load": {
                "sourceUris": [FILE_URI],
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": RAW_TABLE,
                },
                "sourceFormat": "CSV",
                "skipLeadingRows": 1,
                "writeDisposition": "WRITE_TRUNCATE",
                "autodetect": True,
            }
        },
        location="US",
        gcp_conn_id="google_cloud_default",
    )

    # 2. Filter for Indian students (NOTE: assuming you’ll add that row later)
    extract_india = BigQueryInsertJobOperator(
        task_id="extract_india",
        configuration={
            "query": {
                "query": f"""
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
                    WHERE LOWER(Country) = 'india'
                """,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": INDIA_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # 3. Filter for UK
    extract_uk = BigQueryInsertJobOperator(
        task_id="extract_uk",
        configuration={
            "query": {
                "query": f"""
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
                    WHERE LOWER(Country) = 'uk'
                """,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": UK_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # 4. Filter for USA
    extract_usa = BigQueryInsertJobOperator(
        task_id="extract_usa",
        configuration={
            "query": {
                "query": f"""
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
                    WHERE LOWER(Country) = 'usa'
                """,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": USA_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # 5. Filter for Australia
    extract_aus = BigQueryInsertJobOperator(
        task_id="extract_aus",
        configuration={
            "query": {
                "query": f"""
                    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{RAW_TABLE}`
                    WHERE LOWER(Country) = 'australia'
                """,
                "destinationTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": AUS_TABLE,
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "useLegacySql": False,
            }
        },
        location="US",
    )

    # DAG flow
    start >> load_raw_data >> [extract_india, extract_uk, extract_usa, extract_aus] >> end
