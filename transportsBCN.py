import pandas as pd
import os
# import urllib.request
from haversine import haversine, Unit # per calcular distancies (m) amb coordenades

""" Descàrrega de dades d'estacions de transport a barcelona """

# info: https://opendata-ajuntament.barcelona.cat/data/ca/dataset/transports
# global_df = pd.read_csv('./Data/global_df.csv')

def count_trans_within_radius(row, est_bicing, radi):
    """
    Per cada fila del data frame compta el num d'estacions de transports
    qui hi ha en el radi definit en metres
    """
    distances = [haversine((row['lat'], row['lon']), (p['LATITUD'], p['LONGITUD']), unit=Unit.METERS)
                 for _, p in est_bicing.iterrows()]
    return len([d for d in distances if d <= radi])

def PublicTransports(global_df):
    """
    descarrega de dades d'estacions de transports publics
    + calcul num d'estacions a 500m
    + merge amb global_df
    """
    url = "https://opendata-ajuntament.barcelona.cat/resources/bcn/GuiaBCN/TRANSPORTS.csv"
    df_trans = pd.read_csv(url)
    estacions_coords = global_df[['station_id','lat','lon']].drop_duplicates()
    estacions_coords['n_transp_500m'] = estacions_coords.apply(count_trans_within_radius, args=(df_trans, 500), axis=1)
    global_df = global_df.merge(estacions_coords, on=['station_id','lat','lon'], how='left')
    return global_df


# test = PublicTransports(global_df)
# print(test.head())