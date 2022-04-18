import os
import logging
from datetime import datetime
import re
from this import d
from unittest import result
import urllib.request
import zipfile
from pathlib import Path
import shutil
import glob

# urllib.request.urlretrieve("https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/historical", "historical.html")
# page_content = ""
# with open("historical.html") as file:
#         page_content = file.read()


TEMP_DIR = './temp/'
URL_PREFIX =  "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/"
TIME_WINDOW = ['historical/', 'recent/']
INDEX = 'index.html'
AVAIL_FILES = 'avail_files.txt'
FILENAME_STATION_DATA = 'KL_Tageswerte_Beschreibung_Stationen.txt'
FILENAME_STATION_DATA_2 = 'KL_Tageswerte_mn4_Beschreibung_Stationen.txt'
FILENAME_BASE_HIST = "tageswerte_KL_00001_19370101_19860630_hist.zip"
#'00424 19540101 19801231             36     52.5000   13.4667 Berlin-Ostkreuz                          Berlin                                                                                            '

# FILENAME_CSV = FILENAME_BASE + ".csv"
# FILENAME_PARQUET = FILENAME_BASE + ".parquet"
# URL_TEMPLATE = URL_PREFIX + FILENAME_CSV

def get_file_content(url, overview_file):
    urllib.request.urlretrieve(url, overview_file)
    with open(overview_file,encoding="ISO-8859-1") as file:
        page_content = file.read()
    return page_content


def download_file(url, filename):
    urllib.request.urlretrieve(url, filename)
    return filename
    
def get_all_filenames(base_url, overview_file):
    filenames = re.findall('href="(tageswerte_KL_[0-9]*_.*.zip)"', get_file_content(base_url, overview_file))
    save_to_file('\n'.join(filenames), AVAIL_FILES)
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

    

    




if __name__ == "__main__":
    # Get the station description
    result = station_data_to_csv(URL_PREFIX+TIME_WINDOW[1], FILENAME_STATION_DATA)
    # Download all data files
    result = get_all_filenames(URL_PREFIX+TIME_WINDOW[1], AVAIL_FILES)
    # print(result)
    download_all_datafiles(URL_PREFIX+TIME_WINDOW[1], AVAIL_FILES)
    # for data_file in result:
    #     download_datafile(URL_PREFIX+TIME_WINDOW[1], data_file)
    # for station in result:
    #     download_datafile(URL_PREFIX+TIME_WINDOW[0], station)
    # result = get_all_filenames(URL_PREFIX+TIME_WINDOW[0], AVAIL_FILES)
    # # result = station_data_to_csv(URL_PREFIX+TIME_WINDOW[0], FILENAME_STATION_DATA)
    # # print(result)
    # print(result[0])
    # download_datafile(URL_PREFIX+TIME_WINDOW[0], result[0])
    # with open('KL_Tageswerte_Beschreibung_Stationen.txt') as file:
    #     ll = file.read()
    #     for l in ll:
    #         print(l)