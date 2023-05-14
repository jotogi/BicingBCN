import os
import urllib.request
import py7zr

ruta_descomprimidos = "./Data/STATIONS/" 

'''
i2m = list(zip(range(1, 13), ['Gener', 'Febrer', 'Marc', 'Abril', 'Maig', 'Juny', 'Juliol', 'Agost', 'Setembre', 'Octubre', 'Novembre', 'Desembre']))

for year in [2022, 2021, 2020, 2019]:
    for month, month_name in i2m:        
        url = f"https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"
        archivo_destino = f"{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"
        
        urllib.request.urlretrieve(url, archivo_destino)
        
        with py7zr.SevenZipFile(archivo_destino, mode='r') as z:
            z.extractall(path=ruta_descomprimidos)
            
        os.remove(archivo_destino)
'''
# i2m = list(zip(range(1, 5), ['Gener', 'Febrer', 'Marc', 'Abril']))
i2m = list(zip(range(3, 13), ['Marc', 'Abril', 'Maig', 'Juny', 'Juliol', 'Agost', 'Setembre', 'Octubre', 'Novembre', 'Desembre']))

for year in [2019]:
    for month, month_name in i2m:        
        url = f"https://opendata-ajuntament.barcelona.cat/resources/bcn/BicingBCN/{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"
        archivo_destino = f"{year}_{month:02d}_{month_name}_BicingNou_ESTACIONS.7z"
        
        urllib.request.urlretrieve(url, archivo_destino)
        
        with py7zr.SevenZipFile(archivo_destino, mode='r') as z:
            z.extractall(path=ruta_descomprimidos)
            
        os.remove(archivo_destino)