import os
from typing import List
import pandas as pd
from logger import get_handler
from DataClean import clean_data_pipeline
from DataReestructure import transform_data_pipeline

# sklearns imports
import sklearn
from sklearn import preprocessing
from sklearn.preprocessing import StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import FeatureUnion
from sklearn import set_config

# to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
set_config(transform_output="pandas")

LOGGER_FILE = 'missages_main.log'
STATIONS_INFO_PATH = './Data/STATIONS/'
STATIONS_INFO_CLEANED_PATH = './Data/STATIONS_CLEANED/'
COLUMNS_TO_DELETE = ['last_reported', 'is_charging_station', 'ttl',
                        'is_installed','is_renting','is_returning', 'status']   
logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

def get_all_files_under_path(path:str='./Data/STATIONS')->List:
    return sorted(os.listdir(path))

def get_reestructured_df_from_file(file:str)->pd.DataFrame:
    # Get df from orginal file
    logger.debug(file)
    try:
        df = pd.read_csv(STATIONS_INFO_PATH+file)

        logger.debug(f'Initial shape: {df.shape}')
        logger.debug(f'Initial columns: {df.columns}')

        # Clean df from bad data
        clean_pipline = clean_data_pipeline(COLUMNS_TO_DELETE)   
        clean_df =clean_pipline.fit_transform(df)

        # Transform df to a reduced df with the target columns
        transformed_pipline = transform_data_pipeline()
        logger.debug(f'Start fit_transform')
        transformed_df =transformed_pipline.fit_transform(clean_df)
        transformed_df.to_csv(STATIONS_INFO_CLEANED_PATH+'CLEANED__'+file)

    except Exception as e:
                logger.debug(f'Error obtaining df from file {file},\nexception missage:\n{str(e)}')
                raise e
    else:
        logger.debug('Data Frame from file {file} -> Completed!')
        

    logger.debug(f'Final shape: {transformed_df.shape}')
    logger.debug(f'Final columns: {transformed_df.columns}')

    return transformed_df


def main():
    dataframe_list = []
    files_list = get_all_files_under_path(STATIONS_INFO_PATH)
    logger.debug(files_list)


    dataframe_list = [get_reestructured_df_from_file(file) for file in files_list]
    logger.debug(f'Number of df: {len(dataframe_list)}')


    # files_list = get_all_files_under_path(STATIONS_INFO_CLEANED_PATH)
    # dataframe_list = [pd.read_csv(STATIONS_INFO_CLEANED_PATH+file) for file in files_list if file != 'global_df.csv']



    globlal_df = dataframe_list[0]
    for index, df in enumerate(dataframe_list):        
        if index == 0:
            pass
        else:
            globlal_df = pd.concat([globlal_df,df], axis=0)
            logger.debug(f'Added df {index}')
        logger.debug(f'El nou dataframe te unes dimensions: {df.shape}')
        logger.debug(f'El dataframe acumulat te unes dimensions: {globlal_df.shape}')
    logger.debug(f'Saving datafrem to csv file. Dimensions {globlal_df.shape}')
    globlal_df.to_csv(STATIONS_INFO_CLEANED_PATH+'global_df.csv')




if __name__ == '__main__':
    main()