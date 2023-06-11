import pandas as pd
from logger import get_handler
from Sequency import sequence
import sklearn
from sklearn import set_config

# to obtain a pandas df to the output of 'fit_transform' instead a numpy arrary
set_config(transform_output="pandas")

LOGGER_FILE = 'missages_main.log'
logger = get_handler(LOGGER_FILENAME= LOGGER_FILE)
logger.info(f'The scikit-learn version should be >=1.2, and is {sklearn.__version__}')

def main():        
    strat_train_set, strat_validation_set = sequence(
        generate_cleaned_files=True, #True if you want to generate all the PRE_ files again. (1h 15min)
        generate_global_df=True, #True if you want to generate all the CLEAN__ files again. (5min)
    )

    
if __name__ == '__main__':
    main()