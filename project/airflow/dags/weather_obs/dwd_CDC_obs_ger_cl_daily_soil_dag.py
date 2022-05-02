import os
import sys
from datetime import datetime

import pyarrow as pa

sys.path.insert(1, '../')
from dag_creation import create_dag


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
    'catchup': False, 
}

DEBUG = False


TEMP_DIR = './temp/'
URL_PREFIX =  'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/'
PRODUCT_CATEGORY = 'soil_temperature/'
TIME_RANGE = ['historical/', 'recent/']
SCHEDULE_INTERVAL = ['0 6 1 * *', '0 6 * * *']
DATA_FOLDER = './data-'+PRODUCT_CATEGORY
AVAIL_FILES = 'avail_files.txt'
FILENAME_STATION_DATA = 'EB_Tageswerte_Beschreibung_Stationen.txt'
FILENAME_BASE = 'produkt_erdbo_tag'
ZIP_FILE_PATTERN = 'tageswerte_EB'

INPUT_FILETYPE = 'parquet'

TABLE_SCHEMA = [
            ('STATIONS_ID',  pa.int64()),
            ('MESS_DATUM',  pa.int64()), 
            ('QN_2',  pa.int32()),  # quality level
            ('V_TE002M',  pa.float64()),    # soil temp in   2cm
            ('V_TE005M',  pa.float64()),    # soil temp in   5cm
            ('V_TE010M',  pa.float64()),    # soil temp in  10cm
            ('V_TE020M',  pa.float64()),    # soil temp in  20cm
            ('V_TE050M',  pa.float64()),    # soil temp in  50cm
            ('eor',  pa.string()),  # end of data record
        ]
        


### Create two dags, one for the historical and one for the recent data
for time_range, schedule_interval in zip(TIME_RANGE, SCHEDULE_INTERVAL):
    dag_id=f'dwd_CDC_obs_ger_cl_daily_{PRODUCT_CATEGORY.strip("/")}_{time_range.strip("/")}_dag'
    
    globals()[dag_id] = create_dag(dag_id=dag_id,
                                   url_prefix=URL_PREFIX,
                                   product_category=PRODUCT_CATEGORY,
                                   filename_station_data=FILENAME_STATION_DATA,
                                   filename_base=FILENAME_BASE,
                                   table_schema=TABLE_SCHEMA,
                                   schedule_interval=schedule_interval,
                                   start_date=datetime.now(),
                                   time_range=time_range,
                                   zip_file_pattern=ZIP_FILE_PATTERN,
                                   default_args=default_args,
                                   debug=DEBUG,
                                   tags=['weather', time_range],)






