from pyproj import Proj
from logger import get_handler
import requests
import json
from DataFilesManager import read_yaml, get_stations_df
import time

logger_geo_bcn = get_handler()
parameters = read_yaml('./parameters.yml')

def get_UTM_from_lon_and_lat(lon:float, lat:float):
    myProj = Proj(proj='utm',zone=31,ellps='WGS84', preserve_units=False)

    # lon, lat = 2.1801069, 41.3979779
    # UTMx, UTMy = 431461.86463044275, 4583262.211126026

    # lon, lat = myProj(UTMx, UTMy, inverse=True)
    # UTMx, UTMy = myProj(lon, lat)

    return myProj(lon, lat)

def get_suburb_code_and_name_from_UTM_coordinates(UTMx:float,UTMy:float,projection:str='EPSG:25831'):
    url = f'https://w33.bcn.cat/geobcn/serveis/territori/barris?x={UTMx}&y={UTMy}&proj="{projection}"'

    response = requests.request("GET", url)
    response_dict = json.loads(response.text)
    if response_dict['estat']=='OK':
        codi = response_dict['resultats'][0]['codi']
        nom = response_dict['resultats'][0]['nom']
    else:
        raise Exception('Suburbs code and name not found')
    
    return codi, nom

def get_suburb_code_and_name_from_Lat_and_Lon_coordinates(lon:float, lat:float):
    UTMx, UTMy = get_UTM_from_lon_and_lat(lon = lon, lat = lat)
    codi, nom = get_suburb_code_and_name_from_UTM_coordinates(UTMx, UTMy)
    print(codi, nom)
    time.sleep(3)
    return codi, nom

def main():
    parameters = read_yaml('./parameters.yml')
    valid_stations = ["station_id","lat","lon","altitude", "address","post_code","capacity",
                      "is_charging_station","nearby_distance","_ride_code_support","rental_uris"]
    
    stations_info_df = get_stations_df(parameters['FILE_STRUCTURE']['INFO']+parameters['FILE_STRUCTURE']['ESTATIONS_FILENAME'],
                                       valid_stations)
        
    stations_info_df['codi_barri'],stations_info_df['nom_barri'] = stations_info_df.apply(lambda x: get_suburb_code_and_name_from_Lat_and_Lon_coordinates(lon = x['lon'], lat = x['lat']), axis=1, result_type='expand')
    
    stations_info_df.to_csv(parameters['FILE_STRUCTURE']['INFO']+'_new_'+parameters['FILE_STRUCTURE']['ESTATIONS_FILENAME'], index=False)
    print(stations_info_df[['codi_barri','nom_barri']])

if __name__ == '__main__':
    main()


