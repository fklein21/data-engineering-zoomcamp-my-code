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

TEMP_DIR = './temp/'
URL_PREFIX =  'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/'
PRODUCT_CATEGORY = 'kl/'
TIME_RANGE = ['historical/', 'recent/']
SCHEDULE_INTERVAL = ['0 6 1 * *', '0 6 * * *']
DATA_FOLDER = './data-'+PRODUCT_CATEGORY
INDEX = 'index.html'
AVAIL_FILES = 'avail_files.txt'
FILENAME_STATION_DATA = 'KL_Tageswerte_Beschreibung_Stationen.txt'
FILENAME_STATION_DATA_2 = 'KL_Tageswerte_mn4_Beschreibung_Stationen.txt'
FILENAME_BASE_HIST = 'tageswerte_KL_00001_19370101_19860630_hist.zip'
FILENAME_BASE = 'produkt_klima_tag'
ZIP_FILE_PATTERN = 'tageswerte_KL'

INPUT_FILETYPE = 'parquet'

TABLE_SCHEMA = [
            ('STATIONS_ID',  pa.int64()),
            ('MESS_DATUM',  pa.int64()), 
            ('QN_3',  pa.int32()),  # quality level
            ('FX',  pa.float64()),   # max of wind gust
            ('FM',  pa.float64()),    # mean wind
            ('QN_4',  pa.int32()),  # quality level of next column
            ('RSK',  pa.float64()), # daily precipitation
            ('RSKF',  pa.int32()),  # precipitation form
            ('SDK',  pa.float64()),  # sunshine duration
            ('SHK_TAG',  pa.float64()),   # snow height (cm)
            ('NM',  pa.float64()),   # mean cloud cover
            ('VPM',  pa.float64()),  # mean vapor pressure
            ('PM',  pa.float64()),   # mean pressure
            ('TMK',  pa.float64()),  # mean temp
            ('UPM',  pa.float64()),  # mean humidity
            ('TXK',  pa.float64()),  # max temp
            ('TNK',  pa.float64()),  # min temp
            ('TGK',  pa.float64()),  # min temp (5cm)
            ('eor',  pa.string()),  # end of data record
        ]



### Create two dags, one for the historical and one for the recent data
for time_range, schedule_interval in zip(TIME_RANGE, SCHEDULE_INTERVAL):
    dag_id=f'dwd_CDC_obs_ger_cl_daily_kl_{time_range.strip("/")}_dag'
    
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
                                   tags=['weather', time_range],)






