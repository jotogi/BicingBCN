import pandas as pd
from logger import get_handler
from DataFilesManager import get_stations_df
from typing import List
import random
import numpy as np
# import os

# sklearns imports
import sklearn
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn import set_config
# from sklearn import preprocessing
# from sklearn.preprocessing import StandardScaler
# from sklearn.preprocessing import FunctionTransformer
# from sklearn.pipeline import FeatureUnion

LOGGER_FILE = 'missages_dataclean.log'
logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

class DeleteNotAvailableStationsRows(BaseEstimator, TransformerMixin):
    def __init__(self, available_stations:List):
        self.available_stations=available_stations
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        '''
        Delete all the rows where a station is not available (NAS).
        
        is_installed == False
        is_renting == False
        is_returning == False 
        status is not IN_SERVICE
        Stations are not in available_stations -> json file
        '''
        try:
            df_filter = ((X['is_installed']==0) | \
                         (X['is_renting']==0)   | \
                         (X['is_returning']==0) | \
                         (~(X['status']=='IN_SERVICE')) | \
                         (~(X['station_id'].isin(self.available_stations))))   

            df_index_to_filter = X[df_filter].index
            logger.debug(f'Index to delete: {len(df_index_to_filter)}')
            X.drop(index=df_index_to_filter,
                #    axis = 1,
                   inplace = True,
                   errors = 'ignore')
        except Exception as e:
            logger.debug(f'Error cleaning the rows for NAS, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('DeleteNotAvailableStationsRows -> Completed!')
        return X

class DeleteNaNInRows(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            # X[~np.isnan(X).any(axis=1)]
            X.dropna(axis=1)
        except Exception as e:
            logger.debug(f'Error cleaning the rows for NaN, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('DeleteNaNInRows -> Completed!')
        return X

class TransformToTimestamp(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X['last_updated'] = X['last_updated'].apply(lambda x: pd.Timestamp(x, unit='s',tz='Europe/Madrid'))
            X['year'] = X['last_updated'].dt.year
            X['month'] = X['last_updated'].dt.month
            X['day'] = X['last_updated'].dt.day
            X['hour'] = X['last_updated'].dt.hour
            # X['weekend'] = X['last_updated'].apply(lambda x: 0 if x.dayofweek in range(5,6) else 1)
        except Exception as e:
            logger.debug(f'Error casting Timestamp the rows for NaN, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('TransformToTimestamp -> Completed!')        
        return X

class MergeStationsWithInfo(BaseEstimator, TransformerMixin):
    def __init__(self, info:pd.DataFrame):
        self.info = info
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X = X.merge(right=self.info, on='station_id')
        except Exception as e:
            logger.debug(f'Error merging stations with info, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('MergeStationsWithInfo -> Completed!')        
        return X

class CreateRelativeOccupacyColumn(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            # X['calculated_capacity']=X['num_bikes_available']+X['num_docks_available']
            # X['max_capacity']=X[['capacity','calculated_capacity']].max(axis=1)            
            # X['percentage_docks_available'] = X['num_docks_available']/(X['max_capacity'])
            X['percentage_docks_available'] = X['num_docks_available']/(X['num_bikes_available']+X['num_docks_available'])
        except Exception as e:
            logger.debug(f'Error obtaining relative occupacy, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('CreateRelativeOccupacyColumn -> Completed!')        
        return X

def clean_data_pipeline(columns_to_keep:List,
                        valid_stations_id:List,
                        stations_info:pd.DataFrame)->Pipeline:

    # Instantiacte transformers
    clean_NAS = DeleteNotAvailableStationsRows(valid_stations_id)

    delete_columns_transformer = ColumnTransformer(
        [# Ordered transformations
        ('Select', 'passthrough' ,columns_to_keep), #-> equivalent to reminder='passthrough'
        # ('DeleteColumns', 'drop' ,columns_to_keep),
        ],
        remainder='drop',
        verbose_feature_names_out=False #-> to avoid changes in the column names.
        )

    clean_NaN = DeleteNaNInRows()
    DateTimeTransform = TransformToTimestamp()
    MergeStationsInfo = MergeStationsWithInfo(info=stations_info)
    RelativeOccupacy = CreateRelativeOccupacyColumn()

    # Instantiate pipeline
    pipeline = Pipeline([
        ('DeleteNAS',clean_NAS),
        ('ColumnTransformer',delete_columns_transformer),
        ('DeleteNaNInRows', clean_NaN),
        ('TransformDateTime', DateTimeTransform),
        ('MergeStationsInfo', MergeStationsInfo),
        ('CreateRelativeOccupaceColumn',RelativeOccupacy),
    ])

    return pipeline


def main():
    df = pd.read_csv('./Data/STATIONS/2021_04_Abril_BicingNou_ESTACIONS.csv')
    STATION_INFO_PATH = './Data/INFO/'
    FILENAME = 'Informacio_Estacions_Bicing.json'
    columns_to_get = ['station_id','lat','lon','capacity']
    stations_info_df = get_stations_df(STATION_INFO_PATH+FILENAME,columns_to_get)

    # to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
    set_config(transform_output="pandas")
    
    valid_stations = random.sample(df['station_id'].unique().tolist(),200)
    logger.debug(f'Valid stations: {valid_stations}')
    logger.debug(f'Valid stations length: {len(valid_stations)}')

    logger.debug(f'Initial shape: {df.shape}')
    logger.debug(f'Initial columns: {df.columns}')

    # columns_to_delete = ['last_reported', 'is_charging_station', 'ttl',
    #                      'is_installed','is_renting','is_returning', 'status']    
    columns_to_keep = ['station_id','last_updated','num_bikes_available','num_docks_available']

    clean_pipline = clean_data_pipeline(columns_to_keep=columns_to_keep,
                                        valid_stations_id=valid_stations,
                                        stations_info=stations_info_df)

    logger.debug(f'Start fit_transform')

    clean_df =clean_pipline.fit_transform(df)

    logger.debug(f'Final stations: {np.unique(clean_df.station_id)}')
    logger.debug(f'Final shape: {clean_df.shape}')
    logger.debug(f'Final columns: {clean_df.columns}')



if __name__ == '__main__':
    main()