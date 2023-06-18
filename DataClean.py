import pandas as pd
from logger import get_handler
from DataFilesManager import read_yaml, get_all_files_under_path_sorted, create_folder, get_stations_df
from typing import List
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn import set_config

# to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
set_config(transform_output="pandas")

logger = get_handler()
parameters = read_yaml('./parameters.yml')

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
            X['minute'] = X['last_updated'].dt.minute
            X['weekend'] = X['last_updated'].apply(lambda x: 0 if x.dayofweek in range(5,7) else 1)
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

def get_cleaned_data_df_from_df(df:pd.DataFrame,                                   
                                valid_stations:List,
                                stations_info_df:pd.DataFrame)->pd.DataFrame:
    '''
    Get original file 'file' from 'origin_path' and pass all the defined pipelines,
    returning the cleaned df.
    '''
    try:
        # Clean df from bad data
        clean_pipeline = clean_data_pipeline(columns_to_keep=parameters['COLUMNS']['COLUMNS_TO_KEEP'],
                                             valid_stations_id=valid_stations,
                                             stations_info=stations_info_df)
        clean_df =clean_pipeline.fit_transform(df)

    except Exception as e:
                logger.debug(f'Error cleaning dataframe,\nexception missage:\n{str(e)}')
                raise e
    else:
        logger.debug('Data cleaning -> Completed!')

    return clean_df

def remove_data_from_other_month(df:pd.DataFrame, month:int)->None:
    index_to_drop = df[df['month']!=month].index
    df.drop(labels=index_to_drop, axis=0, inplace=True)
    return df

def clean_data_sequence()->None:
    # get dirty files (originals from web {year}_{month}_{month_name}_%Bicing%_INFORMACIO.csv)
    # get stations dataframe.
    # get list of valid stations.
    # create folder to save results if doesn't exists.
    # convert files in dataframes.
    # clean dataframes
    # remove data from other months
    # save dataframe to csv.

    dirty_files = [file for file in get_all_files_under_path_sorted(parameters['FILE_STRUCTURE']['STATIONS_INFO_PATH']) if file.split('.')[1] == 'csv']
    stations_info_df = get_stations_df(parameters['FILE_STRUCTURE']['INFO']+parameters['FILE_STRUCTURE']['ESTATIONS_FILENAME'],
                                       parameters['COLUMNS']['COLUMNS_TO_GET_FROM_INFO'])
    valid_stations = stations_info_df['station_id'].unique().tolist()
    create_folder(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH'])


    for file in dirty_files:
        month = int(file.split('_')[1])
        df = pd.read_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_PATH']+file)
        clean_df = get_cleaned_data_df_from_df(df, valid_stations, stations_info_df)
        clean_df = remove_data_from_other_month(clean_df,month)
        clean_df.to_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+'PRE_'+file, index=False)