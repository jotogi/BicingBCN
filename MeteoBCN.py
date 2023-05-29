# Import libraries
import pandas as pd
import numpy as np
import os
from tqdm.notebook import tqdm

from scipy.spatial import distance_matrix

#Estacions meteo automàtiques de Barcelona
estacions_meteo_BCN = [('X4', 'Raval', '41.3839', '2.16775'),
('X8', 'ZonaUniversitaria','41.37919', '2.1054'),
#('X2', 'Ciutadella', '41.38943','2.18847'),
('D5', 'Tibidabo', '41.41843','2.12388')]

estacions_meteo_BCN = pd.DataFrame(estacions_meteo_BCN, columns = ['wstation_id', 'name', 'lat', 'lon'])
estacions_meteo_BCN['lat'] = estacions_meteo_BCN['lat'].astype(float)
estacions_meteo_BCN['lon'] = estacions_meteo_BCN['lon'].astype(float)

## per les queries a les dades del meteocat
import requests
key = 'your key......'
url0 = 'https://api.meteo.cat/xema/v1/estacions/mesurades'
##

def get_meteocat_day_data(codi, year, month, day):
    """
    Function that downloads the data for a specific day of the year from meteocat website, converts it 
    to a DataFrame and saves it to a csv file for future use.
    NOTE: the number of queries to meteocat site is limited in the time, hence we save the info in csv files
    """
    url_i = url0+'/{}/{}/{:02d}/{:02d}'.format(codi,year,month,day)
    print(url_i)
    response_i = requests.get(url_i, headers={"Content-Type": "application/json", "X-Api-Key": key})

    assert (response_i.status_code == 200), f"response status code: {response_i.status_code}"
    
    dbmet = pd.read_json(response_i.text)
    return  pd.json_normalize(dbmet['variables'][0], record_path=['lectures'],meta=['codi'])

  
def download_meteocat_data_station(path,nom,nid):
    
    """
    Function that downloads from meteocat website the meteocat data for the period of time needed
    and for a specific weather station of the city
    
    """
    m2d = list(zip(range(1,13),[31,28,31,30,31,30,31,31,30,31,30,31])) #any normal
    m2d19 = list(zip(range(3,13),[31,30,31,30,31,31,30,31,30,31])) #de març a desembre
    m2d20 = list(zip(range(1,13),[31,29,31,30,31,30,31,31,30,31,30,31])) #2020 any de traspàs
    m2d23 = list(zip(range(1,4),[31,28,31])) #de gener a març
    d1 = {2019:m2d19,2020:m2d20,2021:m2d,2022:m2d,2023:m2d23}
    YEARS = [2019,2020,2021,2022,2023]
    
    for year in YEARS:
        if not os.path.exists(f'{path}{nom}_{nid}/{year}'):
            os.makedirs(f'{path}{nom}_{nid}/{year}', exist_ok=True)
        for month,ndays in d1[year]:
            df_meteo_mes = pd.concat([get_meteocat_day_data(nid, year, month, day) for day in tqdm(range(1,ndays+1),leave=False)])
            print(df_meteo_mes.shape)
            df_meteo_mes.to_csv(f'{path}{nom}_{nid}/{year}/{year}_{month:02d}_MeteoBCN{nom}_{nid}.csv',index=False)

def download_meteocat_data_all(path='./data_meteo/'):
    for i in range(0,estacions_meteo_BCN.shape[0]):
        download_meteocat_data_station(path, estacions_meteo_BCN.iloc[i,1],estacions_meteo_BCN.iloc[i,0])

        
def transform_meteocat_month_data(df_meteo,stacio_met_id):
    """
    Function that transforms downloaded meteocat data in a format convenient for the purpose of this project 
    """      
    df_new= df_meteo.pivot(index="data", columns="codi", values="valor").rename_axis(columns = None).reset_index()
    columns_to_keep = ['data',36,35,34,33,32,31,30]
    df_new = df_new[columns_to_keep]
    #les dades venen ordenades i per cada 30 minuts. Ens quedem amb les files parells (les hores exactes)
    df_new = df_new.iloc[0:df_new.shape[0]:2]
    #creem el camp ID de l'estació per poder distingir quan les dades estiguin totes juntes en un sol dataframe
    df_new.insert(1,'wstation_id', stacio_met_id)
    #creem els camps year,month,day,hour a partir de data
    df_new.insert(1,'hour',pd.to_datetime(df_new.data).dt.hour)
    df_new.insert(1,'day',pd.to_datetime(df_new.data).dt.day)
    df_new.insert(1,'month',pd.to_datetime(df_new.data).dt.month)
    df_new.insert(1,'year',pd.to_datetime(df_new.data).dt.year)
    df_new = df_new.drop(columns=['data'])
    df_new = df_new.rename(columns={36:'RS',35:'PPT',34:'P',33:'HR',32:'T',31:'DV10',30:'VV10'})
    return df_new      

  
def transform_meteocat_station_data(tosave,path,nom,nid):
    
    YEARS=[2019,2020,2021,2022,2023]
    MONTHS = {2019:range(3,13),2020:range(1,13), 2021:range(1,13), 2022:range(1,13),2023:range(1,4)}

    df = pd.concat([ transform_meteocat_month_data(pd.read_csv(f'{path}{nom}_{nid}/{year}/{year}_{month:02d}_MeteoBCN{nom}_{nid}.csv'),nid) \
        for year in tqdm(YEARS) for month in tqdm(MONTHS[year],leave=False) ])        
    if tosave :
        df.to_csv(f'{path}/All_MeteoBCN{nom}_{nid}.csv',index=False)
    return df
  
def load_meteocat_stations_data(path='./data_meteo/'):
    return pd.concat([read.csv(f'{path}/All_MeteoBCN{nom}_{nid}.csv') for i in range (0,estacions_meteo_BCN.shape[0])])

        
def AssignWeatherStation(dfestacions):
    """
    Function that calculates and assigns the closest barcelona weather station to each bike station
    This information is returned as a new column in dfestacions
    """
    meteo_loc = estacions_meteo_BCN[['lon', 'lat']].values
    bicing_loc = dfestacions[['lon', 'lat']].values
    dist = distance_matrix(bicing_loc, meteo_loc)
    locations = dist.argmin(1)
    dfestacions['wstation_id']=locations #els index estacions_meteo:BCN
    #reemplacem [0,1,...] per ['X4,'X8',..]
    info_estacions['wstation_id'].replace(estacions_meteo_BCN.index.values,estacions_meteo_BCN['wstation_id'],inplace=True)
    return df
    
    return dfestacions

def AssignWeatherVariables(dfbicing,dfmeteovar):
    dfbicing = dfbicing.merge(dfmeteovar, left_on=['year','month','day','hour','wstation_id'],right_on=['year','month','day','hour','wstation_id'])
    return dfbicing
  
  
def main():

    if not os.path.exists(f'./DATA/METEO/'):
            os.makedirs(f'./DATA/METEO/')
    METEOPATH='./DATA/METEO/'
    #download_meteocat_data_all('./DATA/METEO/') #les dades de la web les descarreguen un sol cop i guardem en fitxers
    
    #transform meteocat data into an improved format and saved to .csv file per meteocat station
    for i in range (0,estacions_meteo_BCN.shape[0]):
        transform_meteocat_station_data(True, METEOPATH, estacions_meteo_BCN.iloc[i,1],estacions_meteo_BCN.iloc[i,0])
    meteoDF = load_meteocat_stations_data(METEOPATH)
                        
                   
if __name__=='__main__':
    main()
