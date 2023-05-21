import os, random
from typing import List
import pandas as pd
from logger import get_handler
from DataClean import clean_data_pipeline
from DataReestructure import transform_data_pipeline
from DataFilesManager import get_stations_df


# sklearns imports
import sklearn
from sklearn import set_config

# to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
set_config(transform_output="pandas")

LOGGER_FILE = 'missages_main.log'
INFO = './Data/INFO/'
STATIONS_INFO_PATH = './Data/STATIONS/'
STATIONS_INFO_CLEANED_PATH = './Data/STATIONS_CLEANED/'
FILENAME = 'Informacio_Estacions_Bicing.json'
COLUMNS_TO_GET_FROM_INFO = ['station_id','lat','lon','capacity']
COLUMNS_TO_DELETE = ['last_reported', 'is_charging_station', 'ttl',
                        'is_installed','is_renting','is_returning', 'status']   


logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

def create_folder_structure(folder:str)->None:
    try:    
        os.makedirs('./Data/INFO/')
    except:
        logger.debug(f'Carpeta {folder} ja existeix')
    else:
        logger.debug(f'Carpeta {folder} creada!')

         

def get_all_files_under_path(path:str='./Data/STATIONS')->List:
    return sorted(os.listdir(path))

def get_reestructured_df_from_file(file:str,                                   
                                   valid_stations:List,
                                   stations_info_df:pd.DataFrame,
                                   origin_path:str=STATIONS_INFO_PATH,
                                   results_path:str=STATIONS_INFO_CLEANED_PATH)->pd.DataFrame:
    '''
    Get original file 'file' from 'origin_path' and pass all the defined pipelines,
    returning the cleaned df and saving it as a csv file in 'results_path'.
    '''
    logger.debug(file)
    try:
        df = pd.read_csv(origin_path+file)

        # Clean df from bad data
        # clean_pipline = clean_data_pipeline(COLUMNS_TO_DELETE)   
        clean_pipline = clean_data_pipeline(columns_to_delete=COLUMNS_TO_DELETE,
                                        valid_stations_id=valid_stations,
                                        stations_info=stations_info_df)
        clean_df =clean_pipline.fit_transform(df)

        # Transform df to a reduced df with the target columns
        transformed_pipline = transform_data_pipeline()
        logger.debug(f'Start fit_transform')
        transformed_df =transformed_pipline.fit_transform(clean_df)
        transformed_df.to_csv(results_path+'CLEANED__'+file)

    except Exception as e:
                logger.debug(f'Error obtaining df from file {file},\nexception missage:\n{str(e)}')
                raise e
    else:
        logger.debug('Data Frame from file {file} -> Completed!')

    return transformed_df

def get_global_dataframe(files_path:str,
                         valid_stations:List,
                         stations_info_df:pd.DataFrame,
                         original_files=True,
                         )->pd.DataFrame:
    '''
    Returns a unique dataframe from the files that are in files_path.
    The files are converted to dataframes and concatenated, giving as a result the total dataframe.
    - If original_files = True -> the files used are the original ones.
     Each individual dataframe obtained from his correspondent file are passed through the pipelines.
    - If original_files = False -> we are using intermediate files saved from the dataframes once they
     are passed through the pipelines
    '''

    files_list = get_all_files_under_path(files_path)
    files_list = random.sample(files_list,5) # Only for teste purposes, else comented!

    logger.debug(f'Files to convert: {files_list}')
    if original_files:
        dataframe_list = [get_reestructured_df_from_file(file =file,
                                                         valid_stations = valid_stations,
                                                         stations_info_df = stations_info_df) 
                                                         for file in files_list]
    else:
        dataframe_list = [pd.read_csv(STATIONS_INFO_CLEANED_PATH+file) for file in files_list if file != 'global_df.csv']
    
    return pd.concat(dataframe_list, axis=0)

def run()->pd.DataFrame:
    # Crear aquest systema de fitxers dins de la vostra carpeta de projecte
    create_folder_structure(INFO)
    create_folder_structure(STATIONS_INFO_PATH)
    create_folder_structure(STATIONS_INFO_CLEANED_PATH)
    # Copiar els fitxers de dades dins de la carpeta ./Data/STATIONS

    # El primer cop que generem els df hem de posar 'original_files' = True.
    # Això generearà un arxiu de dades "net" .csv per cada arxiu de dades original
    # Fi només volem generear el dataframe global a partir dels arixius nets ja generats,
    # hem de posar 'original_files' = False.


    stations_info_df = get_stations_df(INFO+FILENAME,COLUMNS_TO_GET_FROM_INFO)
    valid_stations = stations_info_df['station_id'].unique().tolist()


    globlal_df = get_global_dataframe(files_path= STATIONS_INFO_PATH,
                                      original_files=True,
                                      valid_stations=valid_stations,
                                      stations_info_df=stations_info_df)

    # Guardem el dataframe global en format .csv
    globlal_df.to_csv(STATIONS_INFO_CLEANED_PATH+'global_df.csv')
    return globlal_df     



def main():        
     run()

if __name__ == '__main__':
    main()