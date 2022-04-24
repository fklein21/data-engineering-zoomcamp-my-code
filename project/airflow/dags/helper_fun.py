import os
import logging
from datetime import datetime
import glob
from pathlib import Path
import re
import shutil
import zipfile
import urllib.request

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq

### Python functions
TEMP_DIR = 'temp/'

def get_file_content(url, datadir='./', filename=''):
    urllib.request.urlretrieve(url, datadir+filename)
    page_content = ''
    with open(datadir+filename,encoding='ISO-8859-1') as file:
        page_content = file.read()
    return page_content


def download_file(url, filename):
    urllib.request.urlretrieve(url, filename)
    return filename


def save_to_file(text:str, filename:str, flag='w') -> str:
    with open(filename, flag) as file:
        file.write(text)


def delete_file(path):
    if Path(path).exists():
        shutil.rmtree(path)


def create_dir(path):
    if not Path(path).exists():
        os.mkdir(path)


def station_data_to_csv(baseurl, datadir='./', filename='', debug=False):
    if debug:
        logging.info(f'baseurl+filename, datadir+filename: {baseurl+filename, datadir+filename}')
    download_file(baseurl+filename, datadir+filename)
    station_data = []
    with open(datadir+filename, encoding='ISO-8859-1') as file:
        lines = file.read().split('\n')
        ll = iter(lines)
        ll1 = next(ll)
        match = re.findall('(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)\s(\w+)$', ll1)
        for ii in match:
            station_data.append(';'.join(ii))
        next(ll)
        row = next(ll, None)
        while row is not None:
            match = re.findall('([0-9]+)\s*([0-9]+)\s*([0-9]+)\s*([0-9]+)\s*([0-9.]+)\s*([0-9.]+)\s(.*?)\s{2,10000}([\w-]+)$', row.strip())
            for ii in match:
                station_data.append(';'.join(ii))
            if len(match)==0:
                print(f'This line go tnot matched: {row.strip()}')
            row = next(ll, None)
    save_to_file('\n'.join(station_data), datadir+filename)

 
def format_to_parquet(src_file, datadir='./', delimiter=',', table_schema=None,ti=None):
    logging.info(f'Formatting: {datadir+src_file} -> parquet')
    parse_options = pv.ParseOptions(delimiter=delimiter)
    table = pv.read_csv(datadir+src_file, parse_options=parse_options)
    logging.info('Table schema: '+str(table.schema))
    if table_schema is not None:
        table = correct_table_schema(table, table_schema)
    logging.info('Table schema: '+str(table.schema))
    dest_file = src_file.split('.')[0] + '.parquet'
    pq.write_table(table, datadir+dest_file)
    logging.info(f'Formatted to parquet file: {datadir+src_file}')
        

def get_all_filenames(url, datadir='./', filename_file='', zip_file_pattern=''):
    filenames = re.findall(f'href="({zip_file_pattern}.*)"', 
                get_file_content(url, datadir=datadir,filename=filename_file))
    save_to_file('\n'.join(filenames), datadir+filename_file)
    return filenames

    
def download_extract_datafile(base_url, datadir='./', filename='datafile.txt'):
    # download zip, extract csv, delete other files
    logging.info('url: '+base_url+filename)
    delete_file(datadir+TEMP_DIR)
    create_dir(datadir+TEMP_DIR)
    zip_file = datadir+TEMP_DIR+filename
    urllib.request.urlretrieve(base_url+filename, zip_file)
    logging.info('zip_file: '+str(Path(zip_file).resolve()))
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        zip_ref.extractall(datadir+TEMP_DIR)

    csv_file = ''
    for name in glob.glob(datadir+TEMP_DIR+'produkt_*.txt'):
        print(name)
        csv_file = Path(name)
        csv_file.rename(datadir+csv_file.name.replace('.txt', '.csv'))
    delete_file(datadir+TEMP_DIR)


def download_extract_all_datafiles(base_url, datadir='./', filename_file='', debug=False):
    count = 0
    with open(datadir+filename_file, 'r') as file:
        lines = file.read().split('\n')
        for line in lines:
            count += 1
            logging.info('download_extract_all_datafiles: url: '+base_url+line)
            print(line)
            download_extract_datafile(base_url, datadir=datadir, filename=line)
            if count>5 and debug:
                break


def download_all_datafiles(base_url, datadir='./', filename_file='', debug=False):
    with open(datadir+filename_file, 'r') as file:
        lines = file.read().split('\n')
        for filename in lines:
            logging.info('download_all_datafiles: url: '+base_url+filename)
            print(filename)
            # download zip, extract csv, delete other files
            logging.info('url: '+base_url+filename)
            urllib.request.urlretrieve(base_url+filename, datadir+filename.replace('.txt', '.csv'))


def correct_table_schema(table: pa.Table, schema: list):
    # First rename the columns
    columns_new = [x[0] for x in schema]
    table = table.rename_columns(columns_new)
    # Then correct the data type
    return table.cast(pa.schema(schema))


def format_all_csv_to_parquet(datadir='./', delimiter=',', table_schema=None,ti=None):
    for name in glob.glob(datadir+'*.csv'):
        logging.info(f'Formatting csv->parquet: {name}')
        src_file = Path(name)
        parse_options = pv.ParseOptions(delimiter=delimiter)
        table = pv.read_csv(src_file, parse_options=parse_options)
        logging.info('Table schema: '+str(table.schema))
        if table_schema is not None:
            table = correct_table_schema(table, table_schema)
        logging.info('Table schema: '+str(table.schema))

        dest_file = name.replace('.csv', '.parquet')
        pq.write_table(table, dest_file)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_parquet_to_gcs(bucket, datadir='./', ti=None):
    '''
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    '''
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    for name in glob.glob(datadir+'*.parquet'):
        logging.info(f'Uploading: {name}')
        local_file = Path(name)
        # object_name = f'raw/{name.split('/')[-1]}'
        object_name = f'raw/{name}'

        blob = bucket.blob(object_name)
        blob.upload_from_filename(name)


# For debugging: logging input parameters
def log_values(params=[]):
    logging.info('Logging:')
    for pp in params:
        logging.info(str(pp))




# def format_to_parquet(src_file, ti=None, color=''):
#     if not src_file.endswith('.csv'):
#         logging.error('Can only accept source files in CSV format, for the moment')
#         return
#     table = pv.read_csv(src_file)
#     filename_parquet = src_file.replace('.csv', '.parquet')
#     pq.write_table(table, filename_parquet)
#     if ti is not None:
#         ti.xcom_push(key=color+'filename_parquet', value=filename_parquet)




