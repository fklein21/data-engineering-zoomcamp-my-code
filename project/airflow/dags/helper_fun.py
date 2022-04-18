import os
import logging
from datetime import datetime
import glob

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

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


def station_data_to_csv(url, datadir, filename):
    download_file(url+filename, datadir+filename)
    station_data = []
    with open(datadir+filename, encoding="ISO-8859-1") as file:
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
    save_to_file('\n'.join(station_data), datadir+filename)

    
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

def format_to_parquet(src_file, ti=None, color=''):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    filename_parquet = src_file.replace('.csv', '.parquet')
    pq.write_table(table, filename_parquet)
    if ti is not None:
        ti.xcom_push(key=color+'filename_parquet', value=filename_parquet)

def format_csv_to_parquet(ti=None):
    for name in glob.glob('./*.csv'):
        logging.info(f'Formatting csv->parquet: {name}')
        src_file = Path(name)
        table = pv.read_csv(src_file)
        filename_parquet = src_file.replace('.csv', '.parquet')
        pq.write_table(table, filename_parquet)


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_parquet_to_gcs(bucket, ti=None, color='', task_ids="format_to_parquet_task"):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    for name in glob.glob('./*.parquet'):
        logging.info(f'Uploading: {name}')
        object_name = f"raw/{local_file.split('/')[-1]}"

        blob = bucket.blob(object_name)
        blob.upload_from_filename(local_file)



