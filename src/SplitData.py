import numpy as np
from sklearn.model_selection import (StratifiedShuffleSplit,
                                     cross_val_score)



def split_data(df, groups, n_splits=1, test_size=0.2):
    split = StratifiedShuffleSplit(n_splits=n_splits, test_size=test_size, random_state=42)
    for train_index, validation_index in split.split(df, df[groups]):
        strat_train_set = df.iloc[train_index]
        strat_validation_set = df.iloc[validation_index]
    return strat_train_set, strat_validation_set


def evaluate_models(models, X, y):
    rmse_scores=[]
    for model in models:
        scores = cross_val_score(model,
                                 X,
                                 y,
                                 scoring='neg_mean_squared_error')
        rmse_scores.append(np.sqrt(scores))