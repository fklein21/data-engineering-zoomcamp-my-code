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

from helper_fun import format_to_parquet, upload_to_gcs

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
INDEX = 'index.html'
AVAIL_FILES = 'avail_files.txt'
FILENAME_STATION_DATA = 'KL_Tageswerte_Beschreibung_Stationen.txt'
FILENAME_STATION_DATA_2 = 'KL_Tageswerte_mn4_Beschreibung_Stationen.txt'
FILENAME_BASE_HIST = "tageswerte_KL_00001_19370101_19860630_hist.zip"

### Python functions
def get_file_content(url, overview_file):
    urllib.request.urlretrieve(url, overview_file)
    with open(overview_file,encoding="ISO-8859-1") as file:
        page_content = file.read()
    return page_content


def download_file(url, filename):
    urllib.request.urlretrieve(url, filename)
    return filename
    
def get_all_filenames(base_url, filename_file):
    filenames = re.findall('href="(tageswerte_KL_[0-9]*_.*.zip)"', get_file_content(base_url, filename_file))
    save_to_file('\n'.join(filenames), filename_file)
    return filenames

def save_to_file(text:str, filename:str, flag='w') -> str:
    with open(filename, flag) as file:
        file.write(text)


def station_data_to_csv(url,filename):
    download_file(url+filename, filename)
    station_data = []
    with open(filename, encoding="ISO-8859-1") as file:
        lines = file.read().split('\n')
        ll = iter(lines)
        ll1 = next(ll)
        match = re.findall('(\w+)+\s(\w+)+\s(\w+)+\s(\w+)+\s(\w+)+\s(\w+)+\s(\w+)+', ll1)
        for ii in match:
            station_data.append(','.join(ii))
        next(ll)
        row = next(ll, None)
        while row is not None:
            match = re.findall('([0-9]+)\s*([0-9]+)\s*([0-9]+)\s*([0-9]+)\s*([0-9.]+)\s*([0-9.]+)\s(.*?)\s{2,10000}(\w+)', row.strip())
            for ii in match:
                station_data.append(','.join(ii))
            row = next(ll, None)
    save_to_file('\n'.join(station_data), filename)

    
def download_datafile(base_url, filename):
    # download zip, extract csv, delete other files
    data_file = ''
    dirpath = Path(TEMP_DIR)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
    os.mkdir(dirpath)
    urllib.request.urlretrieve(base_url+filename, TEMP_DIR+filename)
    with zipfile.ZipFile(TEMP_DIR+filename, 'r') as zip_ref:
        zip_ref.extractall(TEMP_DIR)
    # shutil.move(TEMP_DIR+'produkt*.txt', './')

    for name in glob.glob(TEMP_DIR+'produkt_*.txt'):
        print(name)
        data_file = Path(name)
        data_file.rename(data_file.name)
    if dirpath.exists() and dirpath.is_dir():
        shutil.rmtree(dirpath)
    return data_file

def download_all_datafiles(base_url, filename_file):
    with open(filename_file, 'r') as file:
        files = file.read().split('\n')
        for file in files:
            print(file)
            download_datafile(base_url, file)




### DAG for downloading the fhv data
# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="dwd_CDC_obs_ger_cl_daily_kl_dag",
    schedule_interval="0 6 * * *",
    start_date=datetime.now(),
    default_args=default_args,
    max_active_runs=1,
    catchup=False, 
    tags=['weather_project']
) as dag:
    pass

    # Download the station data
    download_station_data_task = PythonOperator(
        task_id='download_station_data_task',
        python_callable=station_data_to_csv,
        op_kwargs={
            'url': f'{URL_PREFIX}{PRODUCT_CATEGORY[0]}{TIME_WINDOW[1]}',
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
        task_id='format_csv_to_parquet_task'
        python_callable=format_to_parquet,
        
    )

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


