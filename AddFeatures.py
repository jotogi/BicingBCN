import pandas as pd
from logger import get_handler
from DataLoad import load_holidaysBCN
from MeteoBCN import AssignWeatherVariables, load_meteocat_data
from transportsBCN import PublicTransports
from beachesBCN import DistToBeach

# sklearns imports
import sklearn
from sklearn import set_config
from sklearn.base import BaseEstimator, TransformerMixin # To create full custom transformers
from sklearn.pipeline import Pipeline
from DataFilesManager import read_yaml


LOGGER_FILE = 'missages_datareestrucuture.log'
logger_add_features = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger_add_features.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

parameters = read_yaml('./parameters.yml')


class Weather(BaseEstimator, TransformerMixin):
    # RS-> Radiació solar
    # PPT-> precipitació
    # VV10-> velocitat vent 10 m
    # HR-> Humitat relativa
    # T-> temperatura
    def __init__(self):
        self.meteoDF = load_meteocat_data(parameters['FILE_STRUCTURE']['DATA_METEO'])
        
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        try:
            X = AssignWeatherVariables(X,self.meteoDF)
        except Exception as e:
            logger_add_features.debug(f'Error including Wheather feature to df, exception missage\n{str(e)}')
            raise e
        else:
            logger_add_features.debug('Add wheather features -> Completed!')        
        return X

class CombinedAttributesAdder(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.festes = load_holidaysBCN()
    def fit(self, X, y=None):
        return self  # nothing else to do
    
    def transform(self, X:pd.DataFrame):
        # X['weekend']= pd.to_datetime(X[['year','month','day']]).dt.weekday > 4
        # X['peekhour'] = X.apply(lambda x: 1 if ((x['hour'] in range(8,10)) | (x['hour'] in range(17,19)) ) else 0,axis=1)
        # X['peekhour'] = X['hour'].apply(lambda x: 1 if ((x in range(8,10)) | (x in range(17,19)) ) else 0)
        X['peekhour'] = X.apply(lambda x: 1 if ((x['hour'] in range(8,10)) | (x['hour'] in range(17,19)) |  (x['weekend']==0)) else 0, axis=1)
        # i ara fem producte dfclean22['peekhour'] * dfclean22['workday'] pq només aplica si és laborable
        # X['peekhour']= X.peekhour*(~X.weekend)
        # ara que ja hem actualitzat peekhour, podem convertir weekend -booleà- a integer
        # X.weekend = X.weekend.replace({True: 1, False: 0})
        X['holiday']= pd.to_datetime(X[['day','month','year']]).isin(self.festes)
        X.holiday = X.holiday.replace({True: 1, False: 0})
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
            logger_add_features.debug(f'Error including public transports feature to df, exception missage\n{str(e)}')
            raise e
        else:
            logger_add_features.debug('add public transports feature -> Completed!')        
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
            logger_add_features.debug(f'Error including distance to nearest beach feature to df, exception missage\n{str(e)}')
            raise e
        else:
            logger_add_features.debug('add distance to nearest beach -> Completed!')        
        return X

def add_features_data_pipeline()->Pipeline:

    # Instantiacte transformers
    
    weather_feature = Weather()
    peek_hour_and_hollydays=CombinedAttributesAdder()
    public_transport = Transports()
    dist_to_beach = Beaches()

    # Instantiate pipeline
    pipeline = Pipeline([
        ('Weather',weather_feature),
        ('peek_hour_and_hollydays',peek_hour_and_hollydays),
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
    # logger.debug(type(Features_df))
    # logger.debug(Features_df.columns)
    Features_df.to_csv('./Data/STATIONS_CLEANED/global_df_features.csv', index=False)
    print(Features_df.head())
    print(Features_df.info())
    

if __name__=='__main__':
    main()
