import pandas as pd
import os
# import urllib.request
from haversine import haversine, Unit # per calcular distancies (m) amb coordenades
from beachData import platges

""" Descàrrega de dades de les platges de barcelona """

# info: https://opendata-ajuntament.barcelona.cat/data/ca/dataset/np-platges

def min_dist_to_beach(row, est_bicing):
    """
    Per cada fila del data frame busca el min de la distancia
    """
    distances = [haversine((row['lat'], row['lon']), (p['lat'], p['lon']), unit=Unit.METERS)
                 for _, p in est_bicing.iterrows()]
    return min([d for d in distances])

def DistToBeach(global_df):
    """
    + calcul min dist fin la platja
    + merge amb global_df
    """
    # url = "https://www.bcn.cat/tercerlloc/files/NP-NASIA/opendatabcn_NP-NASIA_Platges-csv.csv"
    # platges = pd.read_csv(url, encoding='latin-1')
    estacions_coords = global_df[['station_id','lat','lon']].drop_duplicates()
    estacions_coords['min_dist_to_beach'] = estacions_coords.apply(lambda row: min_dist_to_beach(row, platges), axis=1)
    global_df = global_df.merge(estacions_coords, on=['station_id','lat','lon'], how='left')
    return global_df
