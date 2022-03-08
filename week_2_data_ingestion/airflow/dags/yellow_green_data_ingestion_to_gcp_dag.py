import os
import logging
from datetime import datetime

from airflow import DAG
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

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
}

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX =  "https://s3.amazonaws.com/nyc-tlc/trip+data/"

COLOR_RANGE = ['yellow', 'green']


### DAG for downloading the yellow taxi data
# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="yellow_green_data_ingestion_gcs_dag",
    schedule_interval="0 6 1 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    default_args=default_args,
    max_active_runs=4,
    catchup=True,
    tags=['dtc-de']
) as dag:

    # Color of the cab
    for color in COLOR_RANGE:

        FILENAME_BASE = f'{color}_tripdata_{{{{ execution_date.strftime(\'%Y-%m\') }}}}'
        FILENAME_CSV = FILENAME_BASE + ".csv"
        FILENAME_PARQUET = FILENAME_BASE + ".parquet"
        URL_TEMPLATE = URL_PREFIX + FILENAME_CSV
        TABLE_NAME_TEMPLATE = f'{color}_taxi_{{{{ execution_date.strftime(\'%Y_%m\') }}}}'

        download_dataset_task = BashOperator(
            task_id=f"download_{color}_dataset_task",
            bash_command=f'curl -sSLf {URL_TEMPLATE} > {AIRFLOW_HOME}/{FILENAME_CSV}'
        )

        delete_csv_task = BashOperator(
            task_id=f"delete_{color}_csv_task",
            bash_command=f'rm -f {AIRFLOW_HOME}/{FILENAME_CSV}'
        )

        format_to_parquet_task = PythonOperator(
            task_id=f"format_{color}_to_parquet_task",
            python_callable=format_to_parquet,
            op_kwargs={
                "src_file": f"{AIRFLOW_HOME}/{FILENAME_CSV}",
                "color": color,
            },
        )

        local_to_gcs_task = PythonOperator(
            task_id=f"local_{color}_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "color": color,
                "task_ids": f"format_{color}_to_parquet_task", 
            },
        )

        delete_parquet_task = BashOperator(
            task_id=f"delete_{color}_parquet_task",
            bash_command=f'rm -f {AIRFLOW_HOME}/{FILENAME_PARQUET}'
        )

        download_dataset_task >> format_to_parquet_task >> delete_csv_task >> local_to_gcs_task >> delete_parquet_task


