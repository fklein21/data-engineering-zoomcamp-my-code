import os
import re
from datetime import datetime
import shutil
import zipfile
import glob
from pathlib import Path
import logging
import urllib.request
from fileinput import filename

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

from helper_fun import format_csv_to_parquet, upload_parquet_to_gcs

### Define default arguments and variables

PROJECT_ID = os.environ.get("GCP_PaROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
}

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX =  "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/"
TIME_WINDOW = ['historical/', 'recent/']
FILENAME_STATION_DATA = 'KL_Tageswerte_Beschreibung_Stationen.txt'
FILENAME_STATION_DATA_2 = 'KL_Tageswerte_mn4_Beschreibung_Stationen.txt'
FILENAME_BASE_HIST = "tageswerte_KL_00001_19370101_19860630_hist.zip"
# FILENAME_CSV = FILENAME_BASE + ".csv"
# FILENAME_PARQUET = FILENAME_BASE + ".parquet"
# URL_TEMPLATE = URL_PREFIX + FILENAME_CSV

TEMP_DIR = './temp/'
URL_PREFIX =  "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/"
PRODUCT_CATEGORY = ['kl/']
TIME_WINDOW = ['historical/', 'recent/']
SCHEDULE_INTERVAL = ['0 6 1 * *', '0 6 * * *']
DATA_FOLDER = './data/'
INDEX = 'index.html'
AVAIL_FILES = 'avail_files.txt'
FILENAME_STATION_DATA = 'KL_Tageswerte_Beschreibung_Stationen.txt'
FILENAME_STATION_DATA_2 = 'KL_Tageswerte_mn4_Beschreibung_Stationen.txt'
FILENAME_BASE_HIST = "tageswerte_KL_00001_19370101_19860630_hist.zip"



### DAG for downloading the recent weather data
# NOTE: DAG declaration - using a Context Manager (an implicit way)

for time_window, schedule_interval in zip(TIME_WINDOW, SCHEDULE_INTERVAL):

    with DAG(
        dag_id=f"dwd_CDC_obs_ger_cl_daily_kl_{time_window}_dag",
        schedule_interval=schedule_interval,
        start_date=datetime.now(),
        default_args=default_args,
        max_active_runs=1,
        catchup=False, 
        tags=['weather_project', time_window]
    ) as dag:

        # Download the station data
        download_station_data_task = PythonOperator(
            task_id='download_station_data_task',
            python_callable=station_data_to_csv,
            op_kwargs={
                'url': f'{URL_PREFIX}{PRODUCT_CATEGORY[0]}{TIME_WINDOW[1]}',
                'datadir': time_window,
                'filename': FILENAME_STATION_DATA,
            }
        )

        # Get the filenames of available files
        get_all_filenames_task = PythonOperator(
            task_id='get_all_filenames_task',
            python_callable=get_all_filenames,
            op_kwargs={
                'base_url': f'{URL_PREFIX}{PRODUCT_CATEGORY[0]}{TIME_WINDOW[1]}', 
                'filename_file': AVAIL_FILES,
            }
        )

        # Download the data 
        download_datafiles_task = PythonOperator(
            task_id='download_datafiles_task',
            python_callable=download_all_datafiles,
            op_kwargs={
                'base_url': f'{URL_PREFIX}{PRODUCT_CATEGORY[0]}{TIME_WINDOW[1]}',
                'filename_file': AVAIL_FILES,
            }
        )

        # Format to parquet
        format_csv_to_parquet_task = PythonOperator(
            task_id='format_csv_to_parquet_task',
            python_callable=format_csv_to_parquet,        
        )

        delete_csv_task = BashOperator(
            task_id="delete_csv_task",
            bash_command=f'rm -f {AIRFLOW_HOME}/*.csv'
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_parquet_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
            },
        )

        # delete_parquet_task = BashOperator(
        #     task_id="delete_parquet_task",
        #     bash_command=f'rm -f {AIRFLOW_HOME}/{FILENAME_PARQUET}'
        # )






        # download_overview_data_task = PythonOperator(
        #     task_id='download_overview_data_task'
        #     python_callable=
        # )

        # download_station_data_task = BashOperator(
        #     task_id="download_station_data_task"
        #     bash_command=f'curl -sSLf {URL_TEMPLATE} > {AIRFLOW_HOME}/{FILENAME_CSV}'
        # )
        # download_dataset_task = BashOperator(
        #     task_id="download_dataset_task",
        #     bash_command=f'curl -sSLf {URL_TEMPLATE} > {AIRFLOW_HOME}/{FILENAME_CSV}'
        # )

        # delete_csv_task = BashOperator(
        #     task_id="delete_csv_task",
        #     bash_command=f'rm -f {AIRFLOW_HOME}/{FILENAME_CSV}'
        # )

        # format_to_parquet_task = PythonOperator(
        #     task_id="format_to_parquet_task",
        #     python_callable=format_to_parquet,
        #     op_kwargs={
        #         "src_file": f"{AIRFLOW_HOME}/{FILENAME_CSV}",
        #     },
        # )

        # local_to_gcs_task = PythonOperator(
        #     task_id="local_to_gcs_task",
        #     python_callable=upload_to_gcs,
        #     op_kwargs={
        #         "bucket": BUCKET,
        #     },
        # )

        # delete_parquet_task = BashOperator(
        #     task_id="delete_parquet_task",
        #     bash_command=f'rm -f {AIRFLOW_HOME}/{FILENAME_PARQUET}'
        # )

        # download_dataset_task >> format_to_parquet_task >> delete_csv_task >> local_to_gcs_task >> delete_parquet_task
        download_station_data_task >> get_all_filenames_task >> download_datafiles_task


