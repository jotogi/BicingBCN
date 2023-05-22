# Import libraries
import pandas as pd
import numpy as np
import os

from scipy.spatial import distance_matrix

#Estacions meteo automàtiques de Barcelona
estacions_meteo_BCN_list = [('X4', 'Raval', '41.3839', '2.16775'),
('X8', 'Zona Universitaria','41.37919', '2.1054'),
('AN', 'Ciutadella', '41.39004','2.18091'),
('D5', 'Tibidabo', '41.41843','2.12388')]

estacions_meteo_BCN = pd.DataFrame(estacions_meteo_BCN_list, columns = ['weather_station_ID', 'name', 'lat', 'lon'])
estacions_meteo_BCN['lat'] = estacions_meteo_BCN['lat'].astype(float)
estacions_meteo_BCN['lon'] = estacions_meteo_BCN['lon'].astype(float)

def AssignWeatherStation(dfestacions):
  
    meteo_loc = estacions_meteo_BCN[['lon', 'lat']].values
    bicing_loc = dfestacions[['lon', 'lat']].values
    dist = distance_matrix(bicing_loc, meteo_loc)
    locations = dist.argmin(1)
    dfestacions['weather_station']=locations
    dfestacions['weather_station'].replace([0,1,2,3],['X4','X8','AN','D5'],inplace=True)
    
    return df
