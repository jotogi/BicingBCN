from logger import get_handler
from Sequency import sequence
from Trainer import generate_train_test_df
import sklearn
from sklearn import set_config
import pandas as pd

# to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
set_config(transform_output="pandas")



LOGGER_FILE = 'missages_main.log'
logger_main = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger_main.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

def main():        
    strat_train_set, strat_validation_set = sequence(
        generate_cleaned_files=False, #True if you want to generate all the PRE_ files again. (1h 15min)
        generate_global_df=False, #True if you want to generate all the CLEAN__ files again. (5min)
        generate_train_and_test_sets=False, #True if you want to generate all train and test files again. 
    )

    transformed_train_set, transformed_validation_set = generate_train_test_df(strat_train_set, strat_validation_set)
    print(transformed_train_set.shape, transformed_validation_set.shape)




    
if __name__ == '__main__':
    main()