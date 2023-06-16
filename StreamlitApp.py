import streamlit as st
import pandas as pd
import folium
from folium.plugins import MarkerCluster

# Cargar el archivo CSV
df = pd.read_csv('./Data/STATIONS_CLEANED/global_df_features.csv', nrows = 50000)

# Convertir campos a fecha
df['fecha'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])

# Configurar la paleta de colores de la escala de verde a rojo
color_scale = ['#00FF00', '#FF0000']

# Crear el mapa centrado en la ubicación promedio
mapa = folium.Map(location=[df['lat'].mean(), df['lon'].mean()], zoom_start=10)

# Crear el marcador de agrupación
marker_cluster = MarkerCluster().add_to(mapa)

# Recorrer las filas del dataframe
for index, row in df.iterrows():
    latitud = row['lat']
    longitud = row['lon']
    ratio = row['percentage_docks_available']
    fecha = row['fecha']

    # Verificar si la ratio es un valor faltante (NaN)
    if pd.isnull(ratio):
        continue  # Saltar esta iteración si la ratio es NaN

    # Calcular el índice de color basado en la ratio
    color_index = int((ratio - df['percentage_docks_available'].min()) /
                      (df['percentage_docks_available'].max() - df['percentage_docks_available'].min()) *
                      (len(color_scale) - 1))
    color = color_scale[color_index]

    # Crear un marcador en las coordenadas con el color correspondiente
    folium.CircleMarker([latitud, longitud], radius=5, color=color, fill=True, fill_color=color,
                        popup=f'Ratio: {ratio}<br>Fecha: {fecha}').add_to(marker_cluster)

# Mostrar el mapa en Streamlit
st.title('Visualización de datos con Streamlit')
st.write('Mapa con marcadores agrupados')
st.write(mapa._repr_html_(), unsafe_allow_html=True)
