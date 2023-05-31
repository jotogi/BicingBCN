import pandas as pd
from logger import get_handler
from MeteoBCN import AssignWeatherStation_global_df
from MeteoBCN import AssignWeatherVariables
from MeteoBCN import load_meteocat_stations_data
from transportsBCN import PublicTransports
from beachesBCN import DistToBeach

# sklearns imports
import sklearn
from sklearn import set_config
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline

LOGGER_FILE = 'missages_datareestrucuture.log'
logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

class Weather(BaseEstimator, TransformerMixin):
    def __init__(self,meteoDF=pd.DataFrame()):
        self.meteoDF = load_meteocat_stations_data()
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X = AssignWeatherStation_global_df(X)
            X = AssignWeatherVariables(X,self.meteoDF)
        except Exception as e:
            logger.debug(f'Error including Wheather feature to df, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('Add wheather features -> Completed!')        
        return X

class Transports(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X = PublicTransports(X)
        except Exception as e:
            logger.debug(f'Error including public transports feature to df, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('add public transports feature -> Completed!')        
        return X    

class Beaches(BaseEstimator, TransformerMixin):
    def __init__(self):
        pass
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X = DistToBeach(X)
        except Exception as e:
            logger.debug(f'Error including distance to nearest beach feature to df, exception missage\n{str(e)}')
            raise e
        else:
            logger.debug('add distance to nearest beach -> Completed!')        
        return X    

def add_features_data_pipeline()->Pipeline:

    # Instantiacte transformers
    
    weather_feature = Weather()
    public_transport = Transports()
    dist_to_beach = Beaches()

    # Instantiate pipeline
    pipeline = Pipeline([
        ('Weather',weather_feature),
        ('Transports', public_transport),
        ('Beach', dist_to_beach)
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
    Features_df.to_csv('./Data/STATIONS_CLEANED/global_df_features.csv', index=False)
    print(Features_df.head())
    print(Features_df.info())
    

if __name__=='__main__':
    main()
