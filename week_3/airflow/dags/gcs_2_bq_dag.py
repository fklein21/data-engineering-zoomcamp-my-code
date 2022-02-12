import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, \
    BigQueryInsertJobOperator, BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

# Define default arguments and variables

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

COLOR_RANGE = [("yellow", "tpep_pickup_datetime",), ("green", "lpep_pickup_datetime",), ("fhv", "Pickup_datetime",)]

# : Note: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id='gcs_2_bq_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']
) as dag:

    # Different colors
    for color, col in COLOR_RANGE:

        gcs_2_gcs_task = GCSToGCSOperator(
            task_id=f'{color}_gcs_2_gcs_task',
            source_bucket=BUCKET,
            source_object=f'raw/{color}_tripdata*.parquet',
            destination_bucket=BUCKET,
            destination_object=f'{color}/{color}_tripdata',
            move_object=False,
        )

        create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id=f'{color}_create_bq_dataset', 
            dataset_id=BIGQUERY_DATASET,
            location="europe-west6",
            exists_ok=True,
        )

        gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
            task_id=f'{color}_gcs_2_bq_ext_task',
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f'{color}_tripdata_external_table',
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": 'PARQUET',
                    "sourceUris": [f'gs://{BUCKET}/{color}/*'],
                },
            },
        )

        CREATE_PART_TBL_QUERY = f'CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{color}_tripdata_part \
                                PARTITION BY \
                                    DATE({col}) AS \
                                        SELECT * FROM {BIGQUERY_DATASET}.{color}_tripdata_external_table'
        bq_ext_2_part_task = BigQueryInsertJobOperator(
            task_id=f'{color}_bq_ext_2_part_task',
            configuration={
                "query": {
                    "query": CREATE_PART_TBL_QUERY,
                    "useLegacySql": False,
                }
            }

        )

        gcs_2_gcs_task >> create_bq_dataset >> gcs_2_bq_ext_task >> bq_ext_2_part_task
        

