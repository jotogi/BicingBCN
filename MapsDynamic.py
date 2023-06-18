from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from datetime import date


app = Dash(__name__)


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
                        initial_visible_month=date(2021, 3, 4),
                        date=date(2021, 6, 1))
                ],
                className='label-box-pair'
            ),
            
            # html.Div(
            #     [
            #         html.Label("Select hour:"),
            #         dcc.Dropdown(options=list(range(24)),
            #                      value=0,
            #                      id='time-selection',
            #                      className='dropdown-class'),                    
            #     ],
            #     className='label-box-pair'
            # )
        ],
        className='right-div-class'
    )
])


@app.callback(
    Output("graph", "figure"),
    Input('date-picker-single', 'date'),)
    # Input('station-selection', 'value'),
    # Input('time-selection', 'value'),)
def display_animated_graph(date_value):
    year, month, day = date_value.split('-')
    month_name = {
        1:'Gener',2:'Febrer',3:'Març',4:'Abril',5:'Maig',6:'Juny',
        7:'Juliol',8:'Agost',9:'Setembre',10:'Octubre',11:'Novembre',12:'Desembre',
        }
    df_to_read = pd.read_csv(f'./Data/STATIONS_CLEANED/PRE_{year}_{month}_{month_name[int(month)]}_BicingNou_ESTACIONS.csv')
    df_to_read['Hour:Minute']=df_to_read['hour']+df_to_read['minute']/60
    df_to_read['Hour:Minute']=df_to_read['Hour:Minute'].round(2)
    df_to_read = df_to_read.sort_values(by=['station_id','year','month','day','hour','minute'])

    filter_df = ((df_to_read['year']==int(year)) &
                (df_to_read['month']==int(month)) &
                (df_to_read['day']==int(day))
                )
    df_to_read = df_to_read[filter_df]
    animation = px.density_mapbox(df_to_read,
                            lat='lat', lon='lon', z='percentage_docks_available',
                            radius=5,
                            center=dict(lat=41.40, lon=2.17),
                            zoom=12,
                            opacity=1.0,
                            mapbox_style='open-street-map',
                            height = 1000,
                            animation_frame= 'Hour:Minute',
                            animation_group='station_id'
                            )
    return animation


app.run_server(debug=True)