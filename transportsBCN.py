import pandas as pd
import os
import urllib.request
from haversine import haversine, Unit # per calcular distancies (m) amb coordenades

""" Descàrrega de dades d'estacions de transport a barcelona """

# https://opendata-ajuntament.barcelona.cat/data/ca/dataset/transports

url = "https://opendata-ajuntament.barcelona.cat/resources/bcn/GuiaBCN/TRANSPORTS.csv"
archivo_destino = "./Data/TRANSPORTS.csv"

urllib.request.urlretrieve(url, archivo_destino) # si l'arxiu existeix el remplaça

df_trans = pd.read_csv('./Data/TRANSPORTS.csv')
global_df = pd.read_csv('./Data/global_df.csv')

# dataframe amb els ids de les estacions i coordenades. Per que sigui més rápid el procés
estacions_coords = global_df[['station_id','lat','lon']].drop_duplicates()

def count_trans_within_radius(row, est_bicing, radi):
    """
    Per cada fila del data frame compta el num d'estacions de transports
    qui hi ha en el radi definit en metres
    """
    distances = [haversine((row['lat'], row['lon']), (p['LATITUD'], p['LONGITUD']), unit=Unit.METERS)
                 for _, p in est_bicing.iterrows()]
    return len([d for d in distances if d <= radi])

estacions_coords['n_transp_500m'] = estacions_coords.apply(count_trans_within_radius, args=(df_trans, 500), axis=1)

# global_df_merged = global_df.merge(estacions_coords, on=['station_id','lat','lon'], how='left')
estacions_coords.to_csv('bicing_transports.csv', index=False)

