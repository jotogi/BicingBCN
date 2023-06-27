from dash import Dash, dcc, html, Input, Output
import plotly.express as px
import pandas as pd
from datetime import date
from logger import get_handler
from Stations import get_df_for_dynamics

logger_maps_dynamic = get_handler()

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
            
            html.Div(
                [
                    html.Label("Select feature:"),
                    dcc.Dropdown(options=['Docks available (%)',
                                          '# of bikes leaved',
                                          '# of bikes taken'],
                                 value='Docks available (%)',
                                 clearable=False,
                                 id='feature-selection',
                                 className='dropdown-class')                    
                ],
                className='label-box-pair'
            ),

                        html.Div(
                [
                    html.Label("Avoid until hour:"),
                    dcc.Dropdown(options=list(range(0,24)),
                                 value=0,
                                 clearable=False,
                                 id='hour-selection',
                                 className='dropdown-class')                    
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
    Input('feature-selection', 'value'),
    Input('hour-selection', 'value'),
    )
def display_animated_graph(date_value, feature_value, hour_clean):
    year, month, day = date_value.split('-')
    year_, month_, day_ = int(year), int(month), int(day)
    features = {'Docks available (%)':['percentage_docks_available',1],
                '# of bikes leaved':['number_of_bikes_leaved',100],
                '# of bikes taken':['number_of_bikes_taken',100],
                }
    z_feature = features[feature_value][0]
    max_z_color_scale = features[feature_value][1]

    # month_name = {
    #     1:'Gener',2:'Febrer',3:'Març',4:'Abril',5:'Maig',6:'Juny',
    #     7:'Juliol',8:'Agost',9:'Setembre',10:'Octubre',11:'Novembre',12:'Desembre',
    #     }
    # df_to_read = pd.read_csv(f'./Data/STATIONS_CLEANED/PRE_{year}_{month}_{month_name[int(month)]}_BicingNou_ESTACIONS.csv')
    # df_to_read['Hour:Minute']=df_to_read['hour']+df_to_read['minute']/60
    # df_to_read['Hour:Minute']=df_to_read['Hour:Minute'].round(2)
    # df_to_read = df_to_read.sort_values(by=['station_id','year','month','day','hour','minute'])

    df_to_read =  get_df_for_dynamics(year_,month_,day_, hour_clean)
    filter_df = ((df_to_read['year']==year_) &
                (df_to_read['month']==month_) &
                (df_to_read['day']==day_)
                )
    df_to_read = df_to_read[filter_df]
    # max_z_color_scale = df_to_read[z_feature].max()

    animation = px.density_mapbox(df_to_read,
                            lat='lat', lon='lon', z=z_feature,#z='percentage_docks_available',
                            radius=8,
                            center=dict(lat=41.40, lon=2.17),
                            zoom=12,
                            opacity=1.0,
                            range_color = [0,max_z_color_scale],
                            color_continuous_scale='inferno',
                            # color_continuous_scale=[(0.00, "blue"),   (0.33, "blue"),
                            #                         (0.33, "green"), (0.66, "green"),
                            #                         (0.66, "red"),  (1.00, "red")],
                            mapbox_style='open-street-map',
                            height = 1000,
                            animation_frame= 'Hour:Minute',
                            animation_group='station_id'
                            )
    return animation


app.run_server(debug=True)