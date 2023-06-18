import os
from pathlib import Path
import pandas as pd
from logger import get_handler
from typing import List
import yaml

logger = get_handler()

def read_yaml(file_path):
    with open(file_path, "r") as f:
        return yaml.safe_load(f)

def create_folder(folder:str)->None:
    try:    
        os.makedirs(folder)
    except:
        logger.debug(f'Carpeta {folder} ja existeix')
    else:
        logger.debug(f'Carpeta {folder} creada!')

def get_all_files_under_path_sorted(path:str='./Data/STATIONS')->List:
    return sorted(os.listdir(path))

def get_stations_df(path_to_json, columns_to_get):
    station_info = pd.read_json(path_to_json)
    station_info[columns_to_get] = station_info.apply(lambda x: [x['Estacions'][column_name] for column_name in columns_to_get],axis=1,result_type='expand')
    station_info.drop('Estacions',axis=1, inplace=True)
    return station_info


def download_files():
    i2m = list(zip(range(1,13), ['Gener','Febrer','Marc','Abril','Maig','Juny','Juliol','Agost','Setembre','Octubre','Novembre','Desembre']))
    for year in [2022, 2021, 2020, 2019]:
        for month, month_name in i2m:        
            os.system(f"wget 'https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'")
            os.system(f"7z x '{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'")
            os.system(f"rm '{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z'")

def uncompress_and_delete_file(file):
    os.system(f"7z x './Data/INFO/{file}'")
    os.system(f"rm './Data/INFO/{file}'")

def main():
    files_list = os.listdir('./Data/INFO')
    [uncompress_and_delete_file(file) for file in files_list if os.path.splitext(file)[1]=='.7z']


if __name__=='__main__':
    parameters = read_yaml('./parameters.yml')
    stations_info_df = get_stations_df(parameters['FILE_STRUCTURE']['INFO']+parameters['FILE_STRUCTURE']['ESTATIONS_FILENAME'],
                                       parameters['COLUMNS']['COLUMNS_TO_GET_FROM_INFO'])
    print(stations_info_df.columns)