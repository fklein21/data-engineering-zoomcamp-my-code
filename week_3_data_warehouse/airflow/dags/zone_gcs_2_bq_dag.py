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


# Task for making a table for the zones
# : Note: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id='zone_gcs_2_bq_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de']
) as dag:


    zone_gcs_2_gcs_task = GCSToGCSOperator(
        task_id=f'zone_gcs_2_gcs_task',
        source_bucket=BUCKET,
        source_object=f'raw/taxi+_zone_lookup.parquet',
        destination_bucket=BUCKET,
        destination_object=f'zone/taxi+_zone_lookup.parquet',
        move_object=False,
    )

    zone_create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id=f'zone_create_bq_dataset', 
        dataset_id=BIGQUERY_DATASET,
        location="europe-west6",
        exists_ok=True,
    )

    zone_gcs_2_bq_ext_task = BigQueryCreateExternalTableOperator(
        task_id=f'zone_gcs_2_bq_ext_task',
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f'zone_lookup_external_table',
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": 'PARQUET',
                "sourceUris": [f'gs://{BUCKET}/zone/*'],
            },
        },
    )

    zone_gcs_2_gcs_task >> zone_create_bq_dataset >> zone_gcs_2_bq_ext_task
