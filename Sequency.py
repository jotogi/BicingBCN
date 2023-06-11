import pandas as pd
from DataClean import clean_data_sequence
from DataReestructure import reestructure_data_sequence, read_yaml
from AddFeatures import run_pipeline
from SplitData import split_data
from logger import get_handler

logger = get_handler()
parameters = read_yaml('./parameters.yml')

def sequence(generate_cleaned_files:bool=False, #True if you want to generate all the PRE_ files again.
             generate_global_df:bool=False, #True if you want to generate all the CLEAN__ files again.
             )-> pd.DataFrame:

    # Generate files PRE_ Only DataClean has been applied. The pipeline is:
    #     ('DeleteNAS',clean_NAS),
    #     ('ColumnTransformer',delete_columns_transformer),
    #     ('DeleteNaNInRows', clean_NaN),
    #     ('TransformDateTime', DateTimeTransform),
    #     ('MergeStationsInfo', MergeStationsInfo),
    #     ('CreateRelativeOccupaceColumn',RelativeOccupacy),
    # Finally, all the data that doesn't macth with this file date is removed.    
    if generate_cleaned_files:
        clean_data_sequence()

    # Generate files CLEANED__. In those files, the columns ctx-n are included and the rows pruned.
    # The pipeline is:
    # ('GroupAndAverage',group_and_average),
    # ('ShiftColumns',shift_columns),
    # ('PruneRows',prune_rows),
    # Generate the global_df dataframe and save it on a file (global_df.csv)
    if generate_global_df:
        global_df = reestructure_data_sequence()
    else:
        global_df =  pd.read_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+'global_df.csv')


    # Add new features to the dataframe using the features pipeline:
    # ('Weather',weather_feature),
    # ('Transports', public_transport),
    # ('Beach', dist_to_beach)
    features_df = run_pipeline(global_df)
    features_df.to_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+'features_df.csv')
    
    # Separate the train set and the validation set stratified by 'COLUMNS_TO_STRATIFY'
    strat_train_set, strat_validation_set = split_data(features_df, parameters['COLUMNS']['COLUMNS_TO_STRATIFY'])
    strat_train_set.to_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+'train_set.csv')
    strat_validation_set.to_csv(parameters['FILE_STRUCTURE']['STATIONS_INFO_CLEANED_PATH']+'validation_set.csv')

    return strat_train_set, strat_validation_set