import os
import pandas as pd
import numpy as np
from logger import get_handler
from typing import List

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

LOGGER_FILE = 'missages_dataclean.log'
logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

class DeleteNotAvailableStationsRows(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        '''
        Delete all the rows where a station is not available (NAS).
        
        is_installed == False
        is_renting == False
        is_returning == False 
        status is not IN_SERVICE
        '''
        try:
            df_filter = ((X['is_installed']==0) | \
                         (X['is_renting']==0)   | \
                         (X['is_returning']==0) | \
                         (~(X['status']=='IN_SERVICE')))   

            df_index_to_filter = X[df_filter].index
            logger.debug(f'Index to delete: {len(df_index_to_filter)}')
            X.drop(index=df_index_to_filter,
                   axis = 1,
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
            X['weekend'] = X['last_updated'].apply(lambda x: 0 if x.dayofweek in range(5,6) else 1)
        except Exception as e:
            logger.debug(f'Error casting Timestamp the rows for NaN, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('TransformToTimestamp -> Completed!')        
        return X

class CreateRelativeOccupacyColumn(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X['percentage_docks_available'] = X['num_docks_available']/(X['num_bikes_available']+X['num_docks_available'])
        except Exception as e:
            logger.debug(f'Error obtaining relative occupacy, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('CreateRelativeOccupacyColumn -> Completed!')        
        return X
    


def clean_data_pipeline(columns_to_delete:List)->Pipeline:

    # Instantiacte transformers
    clean_NAS = DeleteNotAvailableStationsRows()

    first_pipeline = ColumnTransformer(
        [# Ordered transformations
        # ('Select', 'passthrough' ,columns_to_keep), -> equivalent to reminder='passthrough'
        ('DeleteColumns', 'drop' ,columns_to_delete),
        ],
        remainder='passthrough',
        verbose_feature_names_out=False #-> to avoid changes in the column names.
        )

    clean_NaN = DeleteNaNInRows()
    DateTimeTransform = TransformToTimestamp()
    RelativeOccupacy = CreateRelativeOccupacyColumn()

    # Instantiate pipeline
    pipeline_all = Pipeline([
        ('DeleteNAS',clean_NAS),
        ('ColumnTransformer',first_pipeline),
        ('DeleteNaNInRows', clean_NaN),
        ('TransformDateTime', DateTimeTransform),
        ('CreateRelativeOccupaceColumn',RelativeOccupacy),
    ])

    return pipeline_all


def main():
    df = pd.read_csv('./Data/STATIONS/2021_04_Abril_BicingNou_ESTACIONS.csv')

    # to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
    set_config(transform_output="pandas")


    logger.debug(f'Initial shape: {df.shape}')
    logger.debug(f'Initial columns: {df.columns}')

    columns_to_delete = ['last_reported', 'is_charging_station', 'ttl',
                         'is_installed','is_renting','is_returning', 'status']    
    clean_pipline = clean_data_pipeline(columns_to_delete)

    logger.debug(f'Start fit_transform')

    clean_df =clean_pipline.fit_transform(df)

    logger.debug(f'Final shape: {clean_df.shape}')
    logger.debug(f'Final columns: {clean_df.columns}')



if __name__ == '__main__':
    main()