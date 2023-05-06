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

LOGGER_FILE = 'missages.log'
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
            print(f'Error cleaning the rows for NAS, exception missage\n{str(e)}')
            raise e
        return X
    
class DeleteNaNInRows(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X[~np.isnan(X).any(axis=1)]
        except Exception as e:
            print(f'Error cleaning the rows for NaN, exception missage\n{str(e)}')
            raise e
        return X


def clean_data(df:pd.DataFrame, columns_to_delete:List)->pd.DataFrame:

    set_config(transform_output="pandas")

    clean_NAS = DeleteNotAvailableStationsRows()

    all_columns = df.columns.to_list()
    columns_to_keep = [column for column in all_columns if column not in columns_to_delete]
    first_pipeline = ColumnTransformer([
    # Ordered transformations
        ('Select', 'passthrough' ,columns_to_keep),
        ('DeleteColumns', 'drop' ,columns_to_delete),
    ])

    clean_NaN = DeleteNaNInRows()

    pipeline_all = Pipeline([
        ('DeleteNAS',clean_NAS),
        ('ColumnTransformer',first_pipeline),
        ('DeleteNaNInRows', clean_NaN),
    ])

    return pipeline_all, columns_to_keep


def main():
    df = pd.read_csv('./Data/STATIONS/2021_04_Abril_BicingNou_ESTACIONS.csv')

    logger.debug(f'Initial shape: {df.shape}')
    logger.debug(f'Initial columns: {df.columns}')

    columns_to_delete = ['last_reported', 'is_charging_station', 'ttl',
                         'is_installed','is_renting','is_returning', 'status']    
    clean_pipline, columns_to_keep = clean_data(df, columns_to_delete)

    clean_df =clean_pipline.fit_transform(df)

    logger.debug(f'Final shape: {clean_df.shape}')
    logger.debug(f'Final columns: {clean_df.columns}')




if __name__ == '__main__':
    main()