import pandas as pd
import numpy as np
import os
from tqdm.notebook import tqdm

from MeteoBCN import assing_weather_station

# Global variables
seed=42

YEARS = [2019,2020,2021,2022] 
i2m = list(zip(range(1,13), ['Gener','Febrer','Març','Abril','Maig','Juny','Juliol','Agost','Setembre','Octubre','Novembre','Desembre']))
i2m19 = list(zip(range(3,13), ['Març','Abril','Maig','Juny','Juliol','Agost','Setembre','Octubre','Novembre','Desembre']))
d1 = {
  2019: i2m19,
  2020: i2m,
  2021: i2m,
  2022: i2m
}

def read_table(year, month, month_name):
    """
    Function that reads the downloaded file and converts it to a DataFrame
    
    """
    return pd.read_csv(f'data_bicing/{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.csv',sep=',')
    
def restore_stations_loc_info():
    """
    Function that reads the already processed file of Informacio_Estacions_Bicing.json and returns it as a DataFrame
    """
    return pd.read_csv(f'data_bicing/Informacio_Estacions_Bicing.csv')
    
def clean_and_process_year(year, dfyear):
    
    #eliminem la columna traffic. En el fitxers dels anys 2019 i 2020 no hi es
    del_columns = []
    if year > 2020: del_columns = ['traffic']
    del_columns = del_columns+['last_reported','ttl']
    dfyear.drop(columns=del_columns,inplace=True)
    
    #eliminem les files en les quals l'estació no està en servei
    df_index_to_filter = dfyear[~(dfyear['status']=='IN_SERVICE')].index
    dfyear.drop(index=df_index_to_filter,inplace = True)
    
    # descifrem la data i creem els camps que necessitarem més endavant
    dfyear['last_updated'] = dfyear['last_updated'].apply(lambda x: pd.Timestamp(x, unit='s',tz='Europe/Madrid'))
 
    dfyear['year'] = dfyear['last_updated'].dt.year
    dfyear['month'] = dfyear['last_updated'].dt.month
    dfyear['day'] = dfyear['last_updated'].dt.day
    dfyear['hour'] = dfyear['last_updated'].dt.hour
    dfyear['min'] = dfyear['last_updated'].dt.minute
    
    #eliminem totes les columnes que ja no ens són necessàries
    dfyear.drop(['is_charging_station','is_installed','is_renting','is_returning','status','last_updated'],axis='columns',inplace=True)
                                
    return dfyear
    
    
def transform(df,dfloc):
    
    #Eliminem les columnes que no ens interessen pel training
    df.drop(columns=['num_bikes_available','num_bikes_available_types.mechanical','num_bikes_available_types.ebike','min'],inplace=True)

    #llegim la localització i demés informció de cada estació (load_info) i fem merge ->filtrarem les estacions
    #que estan al 2023. Pot passar que hi hagi alguna estació nova del 23 i llavors quedarà fora del training també.
    # Els profes han dit el fitxer de predicció no inclourà estacions noves del 2023
    
    df= pd.merge(df,dfloc[['station_id','capacity','altitude']])
    df['percentage_docks_available'] = df.num_docks_available/df.capacity
    df.loc[(dfyear.percentage_docks_available > 1),'percentage_docks_available']=1
    df=df.drop(['num_docks_available'],axis='columns')
    
    df=df.groupby(['station_id','year','month','day','hour'],as_index=False).mean(numeric_only=True) 
    
    #add columns based on previous percentages
    df['ctx-4'] = df['percentage_docks_available'].shift(periods=4, fill_value=0)
    df['ctx-3'] = df['percentage_docks_available'].shift(periods=3, fill_value=0)
    df['ctx-2'] = df['percentage_docks_available'].shift(periods=2, fill_value=0)
    df['ctx-1'] = df['percentage_docks_available'].shift(periods=1, fill_value=0)
    
    #remove intermediate rows
    df_length, initial_row, row_increase = df.shape[0], 4, 5
    df = df.iloc[initial_row:df_length:row_increase]
    
    return df
    
    
def load_bicing(tosave=False):
    """
    Function that reads raw bicing downloaded data, then cleans, processes and joins them in a unique DataFrame
    IMPORTANT: it assumes that all file names follow this format: yyyy_mm_month_name_BicingNou_ESTACIONS.csv
    therefore first files downloaded for 2019 should first be renamed.
    It's assumed that all raw data is stored in the subfolder: data_bicing
    Optionally can save the DataFrame generated by year in a separated file
    """
    allyears=[]
    for year in tqdm(YEARS):
        dfyear = pd.concat([read_table(year, month, month_name) for month, month_name in tqdm(d1[year],leave=False)])
        dfyear = clean_and_process_year(year, dfyear) 
        allyears.append(dfyear)
        if tosave :
            dfyear.to_csv(f'data_bicing/Bicing_{year}_b.csv',index=False)    
    return pd.concat(allyears)
    
def restore_bicing_year(year):
    return pd.read_csv(f'data_bicing/Bicing_{year}.csv')
  
def restore_bicing_all():
    """
    Function that reads the already cleaned bicing data in one file, and converts it to a DataFrame
    """
    try:
        df = pd.read_csv(f'data_bicing/Bicing_All.csv')
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")
        raise
    return df
  
def load_stations_loc_info(tosave=False):
    dfestacions = pd.read_json(f'data_bicing/Informacio_Estacions_Bicing.json',orient='records')
    info_estacions = pd.DataFrame.from_dict( dfestacions['data'][0] )
    # Els camps 'nearby_distance',	'_ride_code_support',	'rental_uris' i	'cross_street' no ens aporten res: 
    #   o bé estan quasi buits o be valen sempre el mateix. Els descartarem 
    
    df = assing_weather_station(df) #creem la columna 'wheather_station'
    df = info_estacions[['station_id','name','lat','lon','altitude','address','post_code','capacity','weather_station']]
    # ho salvem tot
    if tosave:
        df.to_csv(f'data_bicing/Informacio_Estacions_Bicing.csv',index=False)
    return df

def main():
  
    bicing = load_bicing(True)
    stations_loc = load_stations_loc_info(True)
    bicing.to_csv(f'data_bicing/Bicing_All.csv',index=False)
    logger.debug(f'Raw data bicing loaded. Shape: {bicing.shape}')
    bicing = transform(bicing,stations_loc)
    bicing.to_csv(f'data_bicing/Bicing_ToTrain.csv',index=False)
    logger.debug(f'Data bicing ready for training process. Final shape: {bicing.shape}')


if __name__ == '__main__':
    main()
    
    
    
    
    
   
