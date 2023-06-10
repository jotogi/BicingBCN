from dash import Dash, dcc, html, Input, Output
# import dash_bootstrap_components as dbc
# from jupyter_dash import JupyterDash
from datetime import date
# import dash
import plotly.express as px
import pandas as pd

# external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
STATIONS_INFO_CLEANED_PATH = './Data/STATIONS_CLEANED/'

df_to_read = pd.read_csv(STATIONS_INFO_CLEANED_PATH+'global_df.csv', decimal='.')

# app = JupyterDash(__name__, external_stylesheets=external_stylesheets)
app = Dash(name=__name__)

app.layout = html.Div([
    html.H4('Barcelona bike stations'),

    html.Div(
        [
            dcc.Graph(id="graph", className = "graph-class"),
        ],
        className = 'left-div-class'
    ),
    
    html.Div(
        [
            html.Div(
                [
                    html.Label("Select Date:"),
                    dcc.DatePickerSingle(
                        id='date-picker-single',
                        min_date_allowed=date(2019, 3, 1),
                        max_date_allowed=date(2022, 12, 31),
                        initial_visible_month=date(2021, 6, 1),
                        date=date(2021, 6, 1))
                ],
                className='label-box-pair'
            ),
            
            html.Div(
                [
                    html.Label("Select Station:"),
                    dcc.Dropdown(options=list(range(1,519)),
                                 value=1,
                                 id='station-selection',
                                 className='dropdown-class')
                ],
                className='label-box-pair'
            ),

            html.Div(
                [
                    html.Label("Select hour:"),
                    dcc.Dropdown(options=list(range(24)),
                                 value=0,
                                 id='time-selection',
                                 className='dropdown-class'),                    
                ],
                className='label-box-pair'
            )
        ],
        className='right-div-class'
    )
])


@app.callback(
    Output("graph", "figure"),
    Input('date-picker-single', 'date'),
    Input('station-selection', 'value'),
    Input('time-selection', 'value'),
    )
def display_choropleth(date_value,station_value, hour_value):
    year, month, day = date_value.split('-')

    pandas_filter = ((df_to_read['year']==int(year)) &
                     (df_to_read['month']==int(month)) &
                    #  (df_to_read['station_id']==int(station_value)) &
                     (df_to_read['day']==int(day)) &
                     (df_to_read['hour']==hour_value))
    

    fig = px.density_mapbox(df_to_read[pandas_filter],
                            lat='lat', lon='lon', z='percentage_docks_available',
                            # name = 'Available Docks',
                            radius=5,
                            center=dict(lat=41.40, lon=2.17),
                            zoom=12,
                            opacity=0.5,
                            mapbox_style='open-street-map',
                            height = 1000
                            )
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    return fig

# app.run_server(mode="external", debug=True, use_reloader=False)

if __name__ == '__main__':
    app.run_server(debug=True)