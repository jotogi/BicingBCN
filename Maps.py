from dash import Dash, dcc, html, Input, Output
from jupyter_dash import JupyterDash
import dash
import plotly.express as px
import pandas as pd

from DataClean import reduce_dataset

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

df_to_read = pd.read_csv('./Data/INFO/2023_01_Gener_BicingNou_INFORMACIO.csv', decimal='.')

df_to_map = reduce_dataset(df_to_read)

app = JupyterDash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H4('Barcelona bike stations'),
    dcc.Graph(id="graph",
              style={'width': '69%',
                    'display': 'inline-block',}
                    ),

    html.Div([
        html.H6("\nSelect bike capacity:\n"),
        dcc.RangeSlider(0, 54, 7, value=[0, 54], id='my-range-slider-capacity'),
        # html.P('\n\n\n'),
        ],
            style={
            'display': 'inline-block',
            'float': 'right',
            'width': '29%',
#             'padding': '0px 20px 20px 20px',
        })
])


@app.callback(
    Output("graph", "figure"),
    Input('my-range-slider-capacity', 'value')
    )
def display_choropleth(value_capacity):
#     pandas_filter = ((df_to_map['capacity']<=value_capacity[1]) &
#                      (df_to_map['capacity']>=value_capacity[0]))

    # fig = px.density_mapbox(df_to_map[pandas_filter],
    fig = px.density_mapbox(df_to_map,
                            lat='lat', lon='lon', z='capacity',#z='altitude',
                            radius=5,
                            center=dict(lat=41.40, lon=2.17),
                            zoom=12,
                            # mapbox_style="stamen-terrain"
                            mapbox_style='open-street-map',
                            # width = 1000,
                            height = 1000
                            )
    # fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig

app.run_server(mode="external", debug=True, use_reloader=False)