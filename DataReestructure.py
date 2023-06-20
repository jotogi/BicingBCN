import pandas as pd
from logger import get_handler
from DataFilesManager import read_yaml, get_all_files_under_path_sorted
from sklearn import set_config
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline

# to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
set_config(transform_output="pandas")
logger_data_reestructure = get_handler()
parameters = read_yaml('./parameters.yml')


class GroupAndAverage(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            average_df = X.groupby(['station_id','year','month','day','hour']).mean(numeric_only=True)
        except Exception as e:
            logger_data_reestructure.debug(f'Error grouping and transforming, exception missage\n{str(e)}')
            raise e
        else:
            logger_data_reestructure.debug('GroupAndAverage -> Completed!')        
        return average_df
    
class ShiftColumns(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X['ctx-1'] = X['percentage_docks_available'].shift(periods=1, fill_value=0)
            X['ctx-2'] = X['percentage_docks_available'].shift(periods=2, fill_value=0)
            X['ctx-3'] = X['percentage_docks_available'].shift(periods=3, fill_value=0)
            X['ctx-4'] = X['percentage_docks_available'].shift(periods=4, fill_value=0)
        except Exception as e:
            logger_data_reestructure.debug(f'Error shifting columns (ShiftColumns), exception missage\n{str(e)}')
            raise e
        else:
            logger_data_reestructure.debug('ShiftColumns -> Completed!')        
        return X

class PruneRows(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            df_length = X.shape[0]
            initial_row = 4
            row_increase = 5
            pruned_df = X.iloc[initial_row:df_length:row_increase]
        except Exception as e:
            logger_data_reestructure.debug(f'Error pruning rows (PruneRows), exception missage\n{str(e)}')
            raise e
        else:
            logger_data_reestructure.debug('PruneRows -> Completed!')        
        return pruned_df

def transform_data_pipeline()->Pipeline:

    # Instantiacte transformers
    
    group_and_average = GroupAndAverage()
    shift_columns = ShiftColumns()
    prune_rows = PruneRows()

    # Instantiate pipeline
    pipeline = Pipeline([
        ('GroupAndAverage',group_and_average),
        ('ShiftColumns',shift_columns),
        ('PruneRows',prune_rows),
    ])

    return pipeline

def get_reestructured_data_df_from_df(df:pd.DataFrame)->pd.DataFrame:
    try:
        # Transform df to a reduced df with the target columns
        transformed_pipeline = transform_data_pipeline()
        logger_data_reestructure.debug(f'Start fit_transform')
        transformed_df =transformed_pipeline.fit_transform(df)

    except Exception as e:
                logger_data_reestructure.debug(f'Error reestructuring dataframe,\nexception missage:\n{str(e)}')
                raise e
    else:
        logger_data_reestructure.debug('Data reestructuration -> Completed!')

    return transformed_df

def reestructure_data_sequence()->pd.DataFrame:
    # get all the pre-cleaned/processed
    # get dataframes from files
    # apply reestructure pipeline
    # save the resulting dataframe
    # concatenate all the dataframes
    # save the final concatenated df

    cleaned_files = [file for file in get_all_files_under_path_sorted(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']) if 'PRE_' in file]
    all_reestructured_df = []
    for file in cleaned_files:
        df = pd.read_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+file)
        reestructured_df = get_reestructured_data_df_from_df(df)
        reestructured_df.to_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+'CLEANED__'+file.split('PRE_')[1])
        all_reestructured_df.append(reestructured_df)

    globlal_df = pd.concat(all_reestructured_df, axis=0)
    globlal_df.to_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+'global_df.csv')
    return globlal_df