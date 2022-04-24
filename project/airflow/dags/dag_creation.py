import glob
import logging
import os
import re
import shutil
import urllib.request
import zipfile
from datetime import datetime
from email.policy import default
from fileinput import filename
from pathlib import Path
from xml.dom.pulldom import START_DOCUMENT

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import \
    BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from google.cloud import storage

from helper_fun import (download_all_datafiles, format_all_csv_to_parquet, format_to_parquet,
                        get_all_filenames, log_values, station_data_to_csv,
                        upload_parquet_to_gcs, download_extract_all_datafiles)


### Define default arguments and variables
AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'weather_data_all')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'max_active_runs': 1,
    'catchup': True, 
}



TEMP_DIR = './temp/'
AVAIL_FILES = 'avail_files.txt'

INPUT_FILETYPE = 'parquet'



### Creating a dag with schedule_interval and time_range as parameter
def create_dag(dag_id,
               url_prefix,
               product_category,
               filename_station_data,
               filename_base=None,
               table_schema=None,
               schedule_interval=None, 
               start_date=days_ago(1),
               time_range=None,
               zip_file_pattern=None,
               default_args=default_args,
               tags=[],
               debug=False):

    dag = DAG(dag_id,
              schedule_interval=schedule_interval,
              default_args=default_args,
              start_date=start_date,
              tags=tags
              )
    with dag:

        datadir = 'data_'+product_category+'_'+time_range

        # Create data directory
        create_data_dir_task = BashOperator(
            task_id=f'create_data_dir_{time_range.strip("/")}_task',
            bash_command=f'rm -rf {AIRFLOW_HOME}/{datadir} && mkdir {AIRFLOW_HOME}/{datadir}'
        )

        # Download the station data
        download_station_data_task = PythonOperator(
            task_id=f'download_station_data_{time_range.strip("/")}_task',
            python_callable=station_data_to_csv,
            op_kwargs={
                'baseurl': f'{url_prefix}{product_category}{time_range}',
                'datadir': datadir,
                'filename': filename_station_data,
                'debug': debug
            }
        )

        # Format station data to parquet
        format_station_data_to_parquet_task = PythonOperator(
            task_id=f'format_station_data_to_parquet_{time_range.strip("/")}_task',
            python_callable=format_to_parquet,
            op_kwargs={
                'src_file': filename_station_data,
                'datadir': datadir,
                'delimiter': ';',
            }
        )

        # Get the filenames of available files
        get_all_filenames_task = PythonOperator(
            task_id=f'get_all_filenames_{time_range.strip("/")}_task',
            python_callable=get_all_filenames,
            op_kwargs={
                'url': f'{url_prefix}{product_category}{time_range}', 
                'datadir': datadir,
                'filename_file': AVAIL_FILES,
                'zip_file_pattern': zip_file_pattern,
            }
        )

        # Download the data 
        download_extract_all_datafiles_task = PythonOperator(
            task_id=f'download_extract_all_datafiles_{time_range.strip("/")}_task',
            python_callable=download_extract_all_datafiles,
            op_kwargs={
                'base_url': f'{url_prefix}{product_category}{time_range}',
                'datadir': datadir,
                'filename_file': AVAIL_FILES,
                'debug': debug,
            }
        )

        # Format to parquet
        format_all_csv_to_parquet_task = PythonOperator(
            task_id=f'format_all_csv_to_parquet_{time_range.strip("/")}_task',
            python_callable=format_all_csv_to_parquet,    
            op_kwargs={
                'datadir': datadir,
                'table_schema': table_schema,
                'delimiter': ';',
            }    
        )

        # Delete all csv files
        delete_csv_task = BashOperator(
            task_id=f'delete_csv_{time_range.strip("/")}_task',
            bash_command=f'rm -f {AIRFLOW_HOME}/{datadir}/*.csv'
        )

        # Move files to google cloud storage
        local_to_gcs_task = PythonOperator(
            task_id=f'local_to_gcs_{time_range.strip("/")}_task',
            python_callable=upload_parquet_to_gcs,
            op_kwargs={
                'bucket': BUCKET,
                'datadir': datadir,
            },
        )

        # Clean up
        delete_datadir_task = BashOperator(
            task_id=f'delete_datadir_{time_range.strip("/")}_task',
            bash_command=f'rm -rf {AIRFLOW_HOME}/{datadir}'
        )

        # Create external table for station data in Big Query
        bigquery_external_table_station_data_task = BigQueryCreateExternalTableOperator(
            task_id=f'bigquery_external_table_station_data_{time_range.strip("/")}_task',
            table_resource={
                'tableReference': {
                    'projectId': PROJECT_ID,
                    'datasetId': BIGQUERY_DATASET,
                    'tableId': f'{product_category.strip("/")}_{time_range.strip("/")}_station_data_external_table',
                },
                'externalDataConfiguration': {
                    'autodetect': 'True',
                    'sourceFormat': f'{INPUT_FILETYPE.upper()}',
                    'sourceUris': [f'gs://{BUCKET}/raw/{datadir}{filename_station_data.strip(".txt")}*'],
                },
            },
        )

        # Create partitioned table for station data in Big Query
        CREATE_BQ_TBL_QUERY_STATION_DATA = (f'CREATE OR REPLACE TABLE \
            {BIGQUERY_DATASET}.{product_category.strip("/")}_{time_range.strip("/").lower()}_station_data_internal_table \
                AS (\
                    SELECT * FROM \
                        {BIGQUERY_DATASET}.{product_category.strip("/")}_{time_range.strip("/")}_station_data_external_table)'
        )

        # Create a partitioned table for weather data from external table
        bigquery_create_internal_table_station_data_task = BigQueryInsertJobOperator(
            task_id=f'bigquery_create_internal_table_station_data_{time_range.strip("/")}_task',
            configuration={
                'query': {
                    'query': CREATE_BQ_TBL_QUERY_STATION_DATA,
                    'useLegacySql': False,
                }
            }
        )

        # Create external table for weather data in Big Query
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f'bigquery_external_table_{time_range.strip("/")}_task',
            table_resource={
                'tableReference': {
                    'projectId': PROJECT_ID,
                    'datasetId': BIGQUERY_DATASET,
                    'tableId': f'{product_category.strip("/")}_{time_range.strip("/")}_external_table',
                },
                'externalDataConfiguration': {
                    'autodetect': 'True',
                    'sourceFormat': f'{INPUT_FILETYPE.upper()}',
                    'sourceUris': [f'gs://{BUCKET}/raw/{datadir}{filename_base}*'],
                },
            },
        )

        # Create partitioned table for weather data in Big Query
        CREATE_BQ_TBL_QUERY = (f'CREATE OR REPLACE TABLE \
            {BIGQUERY_DATASET}.{product_category.strip("/")}_{time_range.strip("/").lower()}_partitioned_table \
                CLUSTER BY STATIONS_ID AS (\
                    SELECT * FROM \
                        {BIGQUERY_DATASET}.{product_category.strip("/")}_{time_range.strip("/")}_external_table)'
        )

        # Logging task
        logging_task = PythonOperator(
            task_id='logging_task',
            python_callable=log_values,
            op_kwargs={
                'params': [f'{product_category.strip("/")}_{time_range.strip("/")}_external_table',
                f'gs://{BUCKET}/raw/{datadir}*', CREATE_BQ_TBL_QUERY]
            }
        )

        # Create a partitioned table for weather data from external table
        bigquery_create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id=f'bigquery_create_partitioned_table_{time_range.strip("/")}_task',
            configuration={
                'query': {
                    'query': CREATE_BQ_TBL_QUERY,
                    'useLegacySql': False,
                }
            }
        )

        chain(
            logging_task, 
        )

        chain(
            create_data_dir_task, 
            download_station_data_task,
            format_station_data_to_parquet_task,
            delete_csv_task, 
            local_to_gcs_task, 
            delete_datadir_task, 
            bigquery_external_table_station_data_task, 
            bigquery_create_internal_table_station_data_task,
        )

        chain(
            create_data_dir_task, 
            get_all_filenames_task, 
            download_extract_all_datafiles_task, 
            format_all_csv_to_parquet_task, 
            delete_csv_task, 
            local_to_gcs_task, 
            delete_datadir_task, 
            bigquery_external_table_task, 
            bigquery_create_partitioned_table_task,
        )

    return dag
    


#################################################################################################
### Creating a dag with schedule_interval and time_range as parameter
def create_dag_phenology(
               dag_id,
               url_prefix,
               product_category,
               filename_base=None,
               table_schema=None,
               schedule_interval=None, 
               start_date=days_ago(1),
               time_range=None,
               zip_file_pattern=None,
               default_args=default_args,
               tags=[],
               debug=False):

    dag = DAG(dag_id,
              schedule_interval=schedule_interval,
              default_args=default_args,
              start_date=start_date,
              tags=tags
              )
    with dag:

        datadir = 'data_'+product_category.strip('/')+'_'+time_range.strip('/')+'/'

        # Create data directory
        create_data_dir_task = BashOperator(
            task_id=f'create_data_dir_{time_range.strip("/")}_task',
            bash_command=f'rm -rf {AIRFLOW_HOME}/{datadir} && mkdir {AIRFLOW_HOME}/{datadir}'
        )

        # Get the filenames of available files
        get_all_filenames_task = PythonOperator(
            task_id=f'get_all_filenames_{time_range.strip("/")}_task',
            python_callable=get_all_filenames,
            op_kwargs={
                'url': f'{url_prefix}{product_category}{time_range}', 
                'datadir': datadir,
                'filename_file': AVAIL_FILES,
                'zip_file_pattern': zip_file_pattern,
            }
        )

        # Download the data 
        download_all_datafiles_task = PythonOperator(
            task_id=f'download_datafiles_{time_range.strip("/")}_task',
            python_callable=download_all_datafiles,
            op_kwargs={
                'base_url': f'{url_prefix}{product_category}{time_range}',
                'datadir': datadir,
                'filename_file': AVAIL_FILES,
                'debug': debug,
            }
        )

        # Format to parquet
        format_all_csv_to_parquet_task = PythonOperator(
            task_id=f'format_all_csv_to_parquet_{time_range.strip("/")}_task',
            python_callable=format_all_csv_to_parquet,    
            op_kwargs={
                'datadir': datadir,
                'table_schema': table_schema,
                'delimiter': ';',
            }    
        )

        # Delete all csv files
        delete_csv_task = BashOperator(
            task_id=f'delete_csv_{time_range.strip("/")}_task',
            bash_command=f'rm -f {AIRFLOW_HOME}/{datadir}/*.csv'
        )

        # Move files to google cloud storage
        local_to_gcs_task = PythonOperator(
            task_id=f'local_to_gcs_{time_range.strip("/")}_task',
            python_callable=upload_parquet_to_gcs,
            op_kwargs={
                'bucket': BUCKET,
                'datadir': datadir,
            },
        )

        # Clean up
        delete_datadir_task = BashOperator(
            task_id=f'delete_datadir_{time_range.strip("/")}_task',
            bash_command=f'rm -rf {AIRFLOW_HOME}/{datadir}'
        )

        # Create external table for observation data in Big Query
        bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id=f'bigquery_external_table_{time_range.strip("/")}_task',
            table_resource={
                'tableReference': {
                    'projectId': PROJECT_ID,
                    'datasetId': BIGQUERY_DATASET,
                    'tableId': f'{product_category.strip("/")}_{time_range.strip("/")}_external_table',
                },
                'externalDataConfiguration': {
                    'autodetect': 'True',
                    'sourceFormat': f'{INPUT_FILETYPE.upper()}',
                    'sourceUris': [f'gs://{BUCKET}/raw/{datadir}{filename_base}*'],
                },
            },
        )

        # Create partitioned table for observation data in Big Query
        CREATE_BQ_TBL_QUERY = (f'CREATE OR REPLACE TABLE \
            {BIGQUERY_DATASET}.{product_category.strip("/")}_{time_range.strip("/").lower()}_partitioned_table \
                CLUSTER BY STATIONS_ID AS (\
                    SELECT * FROM \
                        {BIGQUERY_DATASET}.{product_category.strip("/")}_{time_range.strip("/")}_external_table)'
        )

        # Logging task
        logging_task = PythonOperator(
            task_id='logging_task',
            python_callable=log_values,
            op_kwargs={
                'params': [f'{product_category.strip("/")}_{time_range.strip("/")}_external_table',
                f'gs://{BUCKET}/raw/{datadir}*', CREATE_BQ_TBL_QUERY]
            }
        )

        # Create a partitioned table for observation data from external table
        bigquery_create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id=f'bigquery_create_partitioned_table_{time_range.strip("/")}_task',
            configuration={
                'query': {
                    'query': CREATE_BQ_TBL_QUERY,
                    'useLegacySql': False,
                }
            }
        )

        chain(
            logging_task, 
        )

        chain(
            create_data_dir_task, 
            get_all_filenames_task, 
            download_all_datafiles_task, 
            format_all_csv_to_parquet_task, 
            delete_csv_task, 
            local_to_gcs_task, 
            delete_datadir_task, 
            bigquery_external_table_task, 
            bigquery_create_partitioned_table_task,
        )

    return dag


