# https://opendata-ajuntament.barcelona.cat/data/ca/dataset/np-platges

# aquest codi no funciona i no vull perdre més temps. així que posso les dates hardcodeadas
# url = "https://www.bcn.cat/tercerlloc/files/NP-NASIA/opendatabcn_NP-NASIA_Platges-csv.csv"
# df_trans = pd.read_csv(url)

import pandas as pd

platges = {'platja': ['Platja de la Mar Bella',
                   'Platja de Sant Sebastià',
                   'Platja de la Nova Icària',
                   'Platja del Somorrostro',
                   'Platja de la Barceloneta',
                   'Platja de la Nova Mar Bella',
                   'Platja de Llevant',
                   'Platja del Bogatell',
                   'Platja de Sant Miquel',
                   'Zona de Banys'],
        'lat': [41.37370, 41.39857, 41.37370, 41.39076, 41.38346, 41.37861, 41.40114, 41.40501, 41.40457, 41.37714],
        'lon': [2.18967, 2.21232, 2.18967, 2.20288, 2.19523, 2.19222, 2.21424, 2.21774, 2.21769, 2.19137]}

platges = pd.DataFrame(platges)