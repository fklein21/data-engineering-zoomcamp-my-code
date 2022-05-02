import os
import sys
from datetime import datetime

import pyarrow as pa

sys.path.insert(1, '../')
from dag_creation import create_dag_phenology


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

DEBUG = False


TEMP_DIR = './temp/'
URL_PREFIX =  'https://opendata.dwd.de/climate_environment/CDC/observations_germany/phenology/immediate_reporters/'
PRODUCT_CATEGORY = 'wild/'
TIME_RANGE = ['historical/', 'recent/']
SCHEDULE_INTERVAL = ['0 6 1 * *', '0 6 1 * *']
AVAIL_FILES = 'avail_files.txt'
DESCRIPTION_DATA = 'PH_Beschreibung_Phasendefinition_Sofortmelder_Wildwachsende_Pflanze.txt'
FILENAME_BASE = 'PH_Sofortmelder_Wildwachsende_Pflanze'
ZIP_FILE_PATTERN = 'PH_Sofortmelder_Wildwachsende_Pflanze'

INPUT_FILETYPE = 'parquet'

TABLE_SCHEMA = [
            ('STATIONS_ID',  pa.string()),
            ('REFERENZJAHR',  pa.string()), 
            ('QUALITAETSNIVEAU',  pa.string()), 
            ('OBJEKT_ID',  pa.string()),        
            ('PHASE_ID',  pa.string()),         
            ('EINTRITTSDATUM',  pa.string()),   
            ('EINTRITTSDATUM_QB',  pa.string()),
            ('JULTAG',  pa.string()),           
            ('eor',  pa.string()),              
            ('empty',  pa.string()),            
        ]


### Create two dags, one for the historical and one for the recent data
for time_range, schedule_interval in zip(TIME_RANGE, SCHEDULE_INTERVAL):
    dag_id=f'dwd_CDC_obs_ger_cl_daily_{PRODUCT_CATEGORY.strip("/")}_{time_range.strip("/")}_dag'
    
    globals()[dag_id] = create_dag_phenology(
                                   dag_id=dag_id,
                                   url_prefix=URL_PREFIX,
                                   product_category=PRODUCT_CATEGORY,
                                   filename_base=FILENAME_BASE,
                                   table_schema=TABLE_SCHEMA,
                                   schedule_interval=schedule_interval,
                                   start_date=datetime.now(),
                                   time_range=time_range,
                                   zip_file_pattern=ZIP_FILE_PATTERN,
                                   default_args=default_args,
                                   debug=DEBUG,
                                   tags=['phenology', PRODUCT_CATEGORY.strip('/'), time_range],)






