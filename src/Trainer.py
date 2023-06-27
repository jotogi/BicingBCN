from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer #per assignar la mitjana ó ... on hi ha NANS

from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import OrdinalEncoder
from sklearn.preprocessing import OneHotEncoder
import numpy as np
import pandas as pd

# variables numèriques
def get_numeric_pipeline()->Pipeline:
    num1_attribs=['altitude','RS','PPT','VV10']
    num1_pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy="median")),
            ('log',FunctionTransformer(np.log1p)),
            ('std_scaler', StandardScaler()),
        ])

    num2_attribs = ['month','hour','HR','T']  
    num2_pipeline = Pipeline([
            ('imputer', SimpleImputer(strategy="median")),
            ('std_scaler', StandardScaler()),
        ])
    
    return Pipeline([
        ('normal', num1_pipeline, num1_attribs),
        ('log', num2_pipeline, num2_attribs),
    ])

num3_attribs=['ctx-4','ctx-3','ctx-2','ctx-1','weekend','peekhour'] #passthrough

#variables categòriques
def transform_Num_to_Cat(data):
    data = data.astype('object') 
    return data

def get_categoric_pipeline():
    cat1_pipeline = Pipeline([
            ('transform_to_Cat',FunctionTransformer(func = transform_Num_to_Cat,validate=False),['station_id']),
            ('imputer', SimpleImputer(strategy="constant",fill_value='Unknown')),
            ('one_hot_encoder', OneHotEncoder(handle_unknown='ignore')),
        ])

    cat2_pipeline = Pipeline([
        ('imputer', SimpleImputer(strategy="constant",fill_value='Unknown')),  
        ('one_hot_encoder', OneHotEncoder(handle_unknown='ignore')),
        ])
    return Pipeline([
        ('station', cat1_pipeline, ['station_id']),
        ('post_code', cat2_pipeline, ['post_code']),
    ])

def generate_train_test_df(train:pd.DataFrame, validate:pd.DataFrame)->pd.DataFrame:
    numerical_pipeline = get_numeric_pipeline()
    categorical_pipeline = get_categoric_pipeline()
    full_pipeline = ColumnTransformer([
        ("num", numerical_pipeline),
        # ("cat", categorical_pipeline),
        ("delete", 'passthrough', num3_attribs)
        ])
    return full_pipeline.fit_transform(train), full_pipeline.transform(validate)




# def main():
#     pass


# if __name__=='__main__':
#     main()