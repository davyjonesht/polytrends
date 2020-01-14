# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
import os

mapper = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11}

paths = []
f = ''
directories = ['L-for-dash/', 'C-for-dash/', 'N-for-dash/', 'Tweet-Count-for-dash/', 'Avg-Senti-for-dash/', 'issues-for-dash/']
for i in directories:
    path = './output/'+i
    for fname in os.listdir(path):
        if fname.endswith('.csv'):
           path = path + fname
    paths.append(path)

def get_min_month(path):
    df = pd.read_csv(path)
    df.month = df.month.map(mapper)
    return df.month.min()

def get_max_month(path):
    df = pd.read_csv(path)
    df.month = df.month.map(mapper)
    return df.month.max()

def get_values(path):
    df = pd.read_csv(path)
    df.month = df.month.map(mapper)
    return df.month.values[0]

def get_trace(path):
    df = pd.read_csv(path)
    pv = pd.pivot_table(df, index=['date'], columns=["sentiment_category"], values=['count'])
    trace1 = go.Bar(x=pv.index, y=pv[('count', 'positive')], name='Positive')
    trace2 = go.Bar(x=pv.index, y=pv[('count', 'neutral')], name='Neutral')
    trace3 = go.Bar(x=pv.index, y=pv[('count', 'negative')], name='Negative')
    return [trace1, trace2, trace3]

layout = go.Layout(barmode='stack', colorway=["#34ebb1", '#f5eb33', '#c91400'])
fig_L = go.Figure(data = get_trace(paths[0]), layout = layout)
fig_C = go.Figure(data = get_trace(paths[1]), layout = layout)
fig_N = go.Figure(data = get_trace(paths[2]), layout = layout)



app = dash.Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.title='#PolyTrends'

navbar = dbc.NavbarSimple(
    children=[],
    brand="#PolyTrends - An insight into the 2019 Canadian Elections!",
    brand_href="#",
    sticky="top",
    color="#3C6478",
    dark=True, 
    brand_style={"text-align":"center", "float":"none", "margin-left":"280px"}
)


tab1_content = dbc.Row(
        [
            html.Img(src=app.get_asset_url('hashtags_word_cloud.jpeg'), style={'width':'100%'}),
        ]
)

tab2_content = dbc.Row(
        [
            html.Img(src=app.get_asset_url('scheer_word_cloud.jpeg'), style={'width':'100%'})
        ]
)

tab3_content = dbc.Row(
        [
            dcc.Graph(
                            figure=fig_L, style={'width':'1000px'}
                        ),
            html.Div([html.H3("Sentiment classification for Liberal Party")], style={"margin-left": "300px"})
        ]
)
        
tab4_content = dbc.Row(
            [
                        dcc.Graph(
                            figure=fig_N, style={'width':'1000px'}
                        ),
            html.Div([html.H3("Sentiment classification for NDP")], style={"margin-left": "300px"})
            ]
        )

tab5_content = dbc.Row(
            [
                        dcc.Graph(
                            figure=fig_C, style={'width':'1000px'}
                        ),
            html.Div([html.H3("Sentiment classification for CPC")], style={"margin-left": "300px"})
            ]
        )

tab6_content = dbc.Row(
            [
                     
                    
                    dbc.Col(dcc.Graph(
                                id='my-graph', style={'width':'1000px'}
                        ), width=9),
                    dbc.Col(html.Div([dcc.Dropdown(id='my-dropdown',options=[{'label': 'NDP', 'value': 'N'},{'label': 'Liberal', 'value': 'L'},{'label': 'Conservative', 'value': 'C'}],
        multi=True,value=['N','L','C'])], style={"margin-top": "250px"}) , width=3),
   
            ]
        )


tab7_content = dbc.Row(
            [
        #html.Div([html.H1("Number of tweets for social issues. - 2019")], style={"textAlign": "center"}),
    dcc.Graph(id="issues-pie-chart", style={'width':'1000px'}),
    html.Div([dcc.Slider(id='month-selected', min=get_min_month(paths[5]), max=get_max_month(paths[5]), value=get_values(paths[5]),
                         marks={1:"January", 2:"February", 3: "March", 4: "April", 5: "May", 6: "June", 7: "July", 8: "August", 9: "September",
                                10: "October", 11: "November", 12: "December"})],
             style={'textAlign': "center", "margin": "30px", "padding": "10px", "width": "65%", "margin-left": "auto",
                    "margin-right": "auto"}),               
#                        dcc.Graph(
#                                id="issues-pie-chart", style={'width':'1000px'}
#                                ),
#                       dcc.Slider(id='month-selected', style={'width':'100px'}, min=get_min_month(paths[5]), max=get_max_month(paths[5]), value=get_values(paths[5]),
#                                           marks={1:"January", 2:"February", 3: "March", 4: "April", 5: "May", 6: "June", 7: "July", 8: "August", 9: "September",
#                                10: "October", 11: "November", 12: "December"})
            ]
        )



tabs=dbc.Tabs(
        [
                dbc.Tab(tab1_content, label="Trending Hashtags", label_style={"color": "#093145"}),
                dbc.Tab(tab3_content, label="Liberal's Sentiment", label_style={"color": "#093145"}),
                dbc.Tab(tab4_content, label="NDP's Sentiment", label_style={"color": "#093145"}),
                dbc.Tab(tab5_content, label="CPC's Sentiment", label_style={"color": "#093145"}),
                dbc.Tab(tab6_content, label="Average Sentiment", label_style={"color": "#093145"}),
                dbc.Tab(tab7_content, label="Trending issues", label_style={"color": "#093145"}),
                dbc.Tab(tab2_content, label="S(c)heer Genius", label_style={"color": "#093145"}),
        ]
        )

body = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(
                    [
                           tabs
                    ],
                    md=12
                )
            ]
        )
    ],
    className="mt-4",
)

app.layout = html.Div([navbar, body])




@app.callback(Output('my-graph', 'figure'),
              [Input('my-dropdown', 'value')])
def update_graph(selected_dropdown_value):
    dropdown = {"N": "NDP","L": "Liberals","C": "CPC",}
    trace1 = []
    trace2 = []
    df = pd.read_csv(paths[4])
    for party in selected_dropdown_value:
        trace1.append(go.Scatter(x=df[df["party"] == party]["date"],y=df[df["party"] == party]["average"],mode='lines',
            opacity=0.7,name=f'{dropdown[party]}',textposition='bottom center'))
        # trace2.append(go.Scatter(x=df[df["party"] == party]["date"],y=df[df["party"] == party]["Close"],mode='lines',
        #     opacity=0.6,name=f'Close {dropdown[party]}',textposition='bottom center'))
    traces = [trace1]
    data = [val for sublist in traces for val in sublist]
    figure = {'data': data,
        'layout': go.Layout(colorway=["#5E0DAC", '#FF4F00', '#375CB1', '#FF7400', '#FFF400', '#FF0056'],
            height=600,title=f"Average Sentiment for {', '.join(str(dropdown[i]) for i in selected_dropdown_value)} Over Time",
            xaxis={"title":"Date",
                   'rangeselector': {'buttons': list([{'count': 1, 'label': '1M', 'step': 'month', 'stepmode': 'backward'},
                                                      {'count': 6, 'label': '6M', 'step': 'month', 'stepmode': 'backward'},
                                                      {'step': 'all'}])},
                   'rangeslider': {'visible': True}, 'type': 'date'},yaxis={"title":"Sentiment"})}
    return figure


@app.callback(
    Output("issues-pie-chart", "figure"),
    [Input("month-selected", "value")]
)
def update_graph(selected):
    df = pd.read_csv(paths[5])
    df.month = df.month.map(mapper)
    #print(df)
    return {
        "data": [go.Pie(labels=df["issue"].unique().tolist(), values=df[df["month"] == selected]["count"].tolist(),
                        marker={'colors': ['#EF963B', '#C93277', '#349600', '#EF533B', '#57D4F1', '#7c7bca', '#fb38c5']}, textinfo='label')],
        "layout": go.Layout(title=f"Tweet segregation across top topics", margin={"l": 300, "r": 300, },
                            legend={"x": 1, "y": 0.7})}



if __name__ == '__main__':
    app.run_server(debug = True)
