import pandas as pd
import numpy as np
import os

from sklearn.metrics import mean_squared_error
from sklearn.ensemble import RandomForestRegressor
#from sklearn.svm import SVR
#from sklearn.tree import DecisionTreeRegressor
from sklearn import neighbors
from sklearn.linear_model import LinearRegression

from sklearn.model_selection import train_test_split

from datetime import datetime

submission_set = pd.read_csv(f'submission/metadata_sample_submission.csv',index_col=0)

def predict_bicing2023(features, model):
    
    #X_test = full_pipeline.transform(submission_set)
    X_pred = submission_set[features]
    y_pred = model.predict(X_pred)
    df_output = pd.DataFrame(y_pred)
    df_output = df_output.reset_index()
    df_output.columns = ['index','percentage_docks_available']
    avui = datetime.today()
    name = 'submission/submission-{}-{}-{}-{}.csv'.format(avui.year,avui.month,avui.day,avui.hour)
    df_output.to_csv(name,index=False)

def train_models(df,features,seed):
    X = df[features]
    y = df['percentage_docks_available'].copy()
    X_train, X_test, y_train, y_test = train_test_split(X,y,test_size=.2,random_state=seed,shuffle=True)
    lin_reg = LinearRegression()
    lin_reg.fit(X_train, y_train)
    y_train_pred_lr = lin_reg.predict(X_train)
    y_test_pred_lr = lin_reg.predict(X_test)

    # Compute MSE for training and testing sets 
    print('MSE (train | test):')
    print(mean_squared_error(y_train_pred_lr, y_train), mean_squared_error(y_test_pred_lr, y_test))
    
    return model
  
def main():
    seed = 42
    dftotrain = pd.read_csv(f'data_bicing/Bicing_ToTrain.csv',index_col=0)  
    #X_test = full_pipeline.transform(dftotrain)
    features=['ctx-4','ctx-3','ctx-2','ctx-1']
    model = train_models(dftotrain,features,seed)
    if not os.path.exists(f'data_bicing/submission'):
        os.makedirs(f'data_bicing/submission', exist_ok=True)
    predict_bicing2023(features,model)
  
