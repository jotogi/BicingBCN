import pandas as pd
from logger import get_handler
from DataClean import clean_data_pipeline

# sklearns imports
import sklearn
from sklearn import set_config
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline
# from sklearn import preprocessing
# from sklearn.preprocessing import StandardScaler
# from sklearn.preprocessing import FunctionTransformer
# from sklearn.compose import ColumnTransformer
# from sklearn.pipeline import FeatureUnion

LOGGER_FILE = 'missages_datareestrucuture.log'
logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

class GroupAndAverage(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            average_df = X.groupby(['station_id','year','month','day','hour']).mean(numeric_only=True)
        except Exception as e:
            logger.debug(f'Error grouping and transforming, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('GroupAndAverage -> Completed!')        
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
            logger.debug(f'Error shifting columns (ShiftColumns), exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('ShiftColumns -> Completed!')        
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
            logger.debug(f'Error pruning rows (PruneRows), exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('PruneRows -> Completed!')        
        return pruned_df

def transform_data_pipeline()->Pipeline:

    # Instantiacte transformers
    
    group_and_average = GroupAndAverage()
    shift_columns = ShiftColumns()
    prune_rows = PruneRows()

    # Instantiate pipeline
    pipeline_all = Pipeline([
        ('GroupAndAverage',group_and_average),
        ('ShiftColumns',shift_columns),
        ('PruneRows',prune_rows),
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
    clean_df =clean_pipline.fit_transform(df)


    transformed_pipline = transform_data_pipeline()
    logger.debug(f'Start fit_transform')
    transformed_df =transformed_pipline.fit_transform(clean_df)

    logger.debug(f'Final shape: {transformed_df.shape}')
    logger.debug(f'Final columns: {transformed_df.columns}')



if __name__ == '__main__':
    main()