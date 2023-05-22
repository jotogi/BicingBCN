import pandas as pd
from logger import get_handler
from MeteoBCN import AssignWeatherStation

# sklearns imports
import sklearn
from sklearn import set_config
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline

LOGGER_FILE = 'missages_datareestrucuture.log'
logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

class Weather(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X = AssignWeatherStation(X)
        except Exception as e:
            logger.debug(f'Error including Wheather feature to df, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('GroupAndAverage -> Completed!')        
        return X
    

def add_features_data_pipeline()->Pipeline:

    # Instantiacte transformers
    
    weather_feature = Weather()

    # Instantiate pipeline
    pipeline = Pipeline([
        ('Weather',weather_feature),
    ])

    return pipeline


def run_pipeline(df:pd.DataFrame)->pd.DataFrame:
    pipeline = add_features_data_pipeline()
    Features_df = pipeline.fit_transform(df)
    return Features_df


def main():
    set_config(transform_output="pandas")
    global_df = pd.read_csv('./Data/STATIONS_CLEANED/global_df.csv')
    Features_df = run_pipeline(global_df)
    logger.debug(type(Features_df))
    logger.debug(Features_df.columns)

    

if __name__=='__main__':
    main()