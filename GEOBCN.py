from pyproj import Proj
import requests
import json

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
    return codi, nom

def main():
    # UTMx, UTMy = get_UTM_from_lon_and_lat(lon = 2.1801069, lat = 41.3979779)
    # codi, nom = get_suburb_code_and_name_from_UTM_coordinates(UTMx, UTMy)

    codi, nom = get_suburb_code_and_name_from_Lat_and_Lon_coordinates(lon = 2.1801069, lat = 41.3979779)

    print(codi, nom)

if __name__ == '__main__':
    main()


