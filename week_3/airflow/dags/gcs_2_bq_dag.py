import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from helper_fun import format_to_parquet, upload_to_gcs

### Define default arguments and variables

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "trips_data_all")

default_args = {
    "owner": "airflow",
    'start_date': days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

###: Note: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id='data_ingestion_gcs_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']
) as dag:
    pass

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id='bigquery_external_table_task',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': 'external_table'
            },
            'exernalDataConfiguration': {
                'sourceFormat': 'PARQUET',
                'dourceUris': [f'gs://{BUCKET}/raw/{parquet_file}'],
            },
        },
    )

