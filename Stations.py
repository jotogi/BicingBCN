import os
import json
import pandas as pd
from logger import get_handler


logger_stations = get_handler()

def prepare_df_from_csv_file(year:int, month:int, day:int)->pd.DataFrame:
    
    month_name = {
            1:'Gener',2:'Febrer',3:'Març',4:'Abril',5:'Maig',6:'Juny',
            7:'Juliol',8:'Agost',9:'Setembre',10:'Octubre',11:'Novembre',12:'Desembre',
            }
    df_to_read = pd.read_csv(f'./Data/STATIONS_CLEANED/PRE_{year}_{month:02d}_{month_name[int(month)]}_BicingNou_ESTACIONS.csv')
    df_to_read['Hour:Minute']=df_to_read['hour']+df_to_read['minute']/60
    df_to_read['Hour:Minute']=df_to_read['Hour:Minute'].round(2)
    df_to_read = df_to_read.sort_values(by=['station_id','year','month','day','hour','minute'])

    filter_df = ((df_to_read['year']==int(year)) &
                (df_to_read['month']==int(month)) &
                (df_to_read['day']==int(day)))

    df_to_read = df_to_read[filter_df]
    return df_to_read

def iterate_over_stations(df:pd.DataFrame, hour:int=0)->pd.DataFrame:
    dataframes_list = []
    for id_station in list(df['station_id'].unique()):
        df_fixed_station = df[(df['station_id']==id_station) & (df['hour']>=hour)][['num_bikes_available']].copy()
        df_fixed_station['number_of_bikes_leaved'] = 0
        df_fixed_station['number_of_bikes_taken'] = 0
        df_fixed_station['diff-inc'] = df_fixed_station['num_bikes_available'].diff(1)
        df_fixed_station['diff-dec'] = df_fixed_station['num_bikes_available'].diff(1)
        df_fixed_station['diff-inc'] = df_fixed_station['diff-inc'].apply(lambda x: 0 if x<0 else x)
        df_fixed_station['diff-dec'] = df_fixed_station['diff-dec'].apply(lambda x: 0 if x>0 else -x)
        # df_fixed_station['diff-inc'].iloc[0] = 0
        # df_fixed_station['diff-dec'].iloc[0] = 0
        df_fixed_station['number_of_bikes_leaved'] = df_fixed_station['diff-inc'].cumsum()
        df_fixed_station['number_of_bikes_taken'] = df_fixed_station['diff-dec'].cumsum()

        dataframes_list.append(df_fixed_station)

    df_fixed_station_concatenated = pd.concat(dataframes_list, axis=0)
    # logger_stations.debug(f'{df_fixed_station_concatenated.columns}')
    df = df.merge(df_fixed_station_concatenated, left_index=True, right_index=True )
    df = df.fillna(0)
    return df

    
     

def get_df_for_dynamics(year:int, month:int, day:int, hour:int=0):
    df_initial = prepare_df_from_csv_file(year, month, day)
    return iterate_over_stations(df_initial, hour)

def main():
    result = get_df_for_dynamics(2021,11,8)
    print(result.columns)


if __name__ == '__main__':
    main()