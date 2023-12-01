import dash
from dash import dcc, html
from dash.dependencies import Input, Output, ClientsideFunction
import dash_bootstrap_components as dbc
import requests
import pandas as pd
import csv
import folium
import matplotlib.pyplot as plt
from folium.plugins import MarkerCluster
from folium.plugins import HeatMap
import json
import urllib3
from urllib3.util.ssl_ import create_urllib3_context
import numpy as np
from concurrent import futures
import os
import plotly.express as px
import plotly.graph_objs as go
from plotly.tools import mpl_to_plotly
import geopandas as gpd


# Criando context para poder burlar erro de certificado ao extrair dados de API PÚBLICAS
ctx = create_urllib3_context()
ctx.load_default_certs()
ctx.options |= 0x4  # ssl.OP_LEGACY_SERVER_CONNECT
http = urllib3.PoolManager(ssl_context=ctx)

# SET MAX_WORKERS
MAX_WORKERS = 20
Dataframes = []

def downloadCsvFile(csv_File):
    df = pd.read_csv(csv_File)
    Dataframes.append(df)

def downloadManyCsvFiles():
    # Fazendo download dos csv's
    urls = ["https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20130101_20131231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20140101_20141231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20150101_20151231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20160101_20161231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20170101_20171231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20180101_20181231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20190101_20191231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20200101_20201231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20210101_20211231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20220101_20221231.csv",
        "https://raw.githubusercontent.com/Josefellype/Trabalho-Prog_Cientifica/main/BasesDeDados/focos_qmd_inpe_20230101_20231115.csv"]

    if os.path.exists("files/dataframes.csv"):
        mergeData = pd.read_csv("files/dataframes.csv")

    else:
        workers = min(MAX_WORKERS, len(urls))
        with futures.ThreadPoolExecutor(workers) as executor:
            res = executor.map(downloadCsvFile, sorted(urls))
            #print(res)
            #return len(list(res))

        # Merge dos dataframes
        mergeData = pd.concat(Dataframes,sort=False)
        mergeData.to_csv("files/dataframes.csv", index=False, encoding="utf-8")

    return mergeData

def generateDataFrameTocantins():
    df_tocantins = gpd.read_file("files/to_setores_2021/TO_Setores_2021.shp")
    df_tocantins['NM_MUN'] = df_tocantins['NM_MUN'].str.title()

    return df_tocantins


def configData(mergedData):
    # Transformando o tipo da coluna DataHora em datetime
    mergedData['DataHora'] = pd.to_datetime(mergedData['DataHora'])

    # Criando colunas YEAR e MONTH
    mergedData['year'] = pd.DatetimeIndex(mergedData['DataHora']).year
    mergedData['month'] = pd.DatetimeIndex(mergedData['DataHora']).month_name()

    # Removendo linhas que não possuam municipio informado
    mergedData = mergedData.loc[mergedData["Municipio"].notnull()]

    # Adicionando coluna incendio para efeito de cálculo
    mergedData["Incendios"] = 1

    # Corrigindo a escrita
    mergedData["Municipio"] = mergedData["Municipio"].str.title()
  
    # Ordenando as informações por data mais recente
    mergedData = mergedData.sort_values(['DataHora'],ascending=False)
    mergedData.drop(mergedData.loc[mergedData['Estado']=="Pernambuco"].index, inplace=True)

    # Trocando NaN por 0.0
    mergedData = mergedData.fillna(0.0)
    months = ["January", "February", "March", "April", "May", "June","July", "August", "September", "October", "November", "December"]
    mergedData['month'] = pd.Categorical(mergedData['month'], categories=months, ordered=True)

    return mergedData

data = downloadManyCsvFiles()
mergedData = configData(data)
geojson = generateDataFrameTocantins()

fogo = mergedData.groupby(["Municipio","year"]).agg({"Incendios":"sum"}).reset_index()
precipitacao = mergedData.groupby(["Municipio","year"]).agg({"Precipitacao":"sum"}).reset_index()


def generate_map(municipio,filtered_df):
    
    lat = filtered_df['Latitude'].mean()
    lon = filtered_df['Longitude'].mean()
    map = folium.Map(location=[lat, lon], zoom_start=8)
    style =  {'fillColor': "blue", #cor de preenchimento
                'color': "red",#cor da linha de contorno
                'weight': 0.5, #espessura da linha
                }
    selected_mun = geojson[geojson['NM_MUN'] == municipio].dissolve(by="CD_MUN")
    folium.GeoJson(selected_mun, style_function=lambda x:style).add_to(map)
        
    return map

# Cria a aplicação 
app = dash.Dash(__name__)

# layout
app.layout = html.Div(children=[
    html.Div(children=[
        html.H1('Análise Qtde de Incêndios por Município'),
        html.P("Selecione um município:"),
        dcc.Dropdown(
            id='municipality',
            options=[{'label': municipio, 'value': municipio} for municipio in fogo['Municipio'].unique()],
            multi=False,
            clearable=True
        ),
        html.Div(className='btn-group', role='group', children=[
            dcc.RadioItems(
                options=[{'label': year, 'value': year} for year in fogo['year'].unique()],
                id="years",
                inline=True,
                
            )
        ]),
        html.Div(children=[
            dcc.Graph(id="graph"),
            dcc.Graph(id="graph2"),
            html.Hr()
        ])
    ]),    
    html.Div([
        dcc.Graph(id='incendios-bar-chart')
    ]),
    html.Div([
        html.Div(dcc.Graph(id="fogo_mensal")),
    ]),
    
    html.Div(id="div_maps", children=[
        html.Div(id="teste",children=[
            html.H4('Distribuição dos incêndios')]),
        html.Div(id="heatMap",children=[
            html.H4('Mapa de calor')]),
        ])
   
    ])


# Callback para atualizar o gráfico com base no município selecionado
@app.callback(
    Output("graph", "figure"), 
    [Input("municipality", "value"),
     Input("years", "value")],prevent_initial_call=True)

def display_choropleth_fire(municipio,year):
    if municipio is not None and year is not None:
        selected_row = fogo[((fogo["Municipio"]==municipio) & (fogo["year"]== year))]
        selected_mun = geojson[geojson['NM_MUN'] == municipio]
        selected_mun = selected_mun.dissolve(by="CD_MUN")
        fig = px.choropleth(
            selected_row, geojson=selected_mun, color='Incendios',
            locations="Municipio", featureidkey="properties.NM_MUN",
            color_continuous_scale="Reds", 
            range_color=[0, selected_row['Incendios'].max()],
            labels="Municipio",
            scope="south america")
        
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

        return fig
    else:
        return {}

# Callback para atualizar o segundo gráfico com base no município selecionado
@app.callback(
    Output("graph2", "figure"), 
    [Input("municipality", "value"),
     Input("years", "value")],prevent_initial_call=True)

def display_choropleth_precipitation(municipio,year):
    if municipio is not None and year is not None:
        selected_row = precipitacao[((precipitacao["Municipio"]==municipio) & (precipitacao["year"]== year))]
        selected_mun = geojson[geojson['NM_MUN'] == municipio]
        selected_mun = selected_mun.dissolve(by="CD_MUN")
        #request = http.request("GET",)
        fig2 = px.choropleth(
            selected_row, geojson=selected_mun, color='Precipitacao',
            scope="south america",
            color_continuous_scale="blues",
            locations="Municipio", 
            featureidkey="properties.NM_MUN",
            range_color=[0, selected_row['Precipitacao'].max()],
            labels="Municipio")
        
        fig2.update_geos(fitbounds="locations", visible=False)
        fig2.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
        
        return fig2
    else:
        return {}

# Callback para atualizar o gráfico com base na seleção do município
@app.callback(
    Output('incendios-bar-chart', 'figure'),
    Input("municipality", "value"),prevent_initial_call=True)

def fire_versus_precipitation_year(selected_municipio):
    filtered_df = fogo[fogo['Municipio'] == selected_municipio]
    precipitacao_municipio = precipitacao[precipitacao['Municipio'] == selected_municipio]

    # Criar o gráfico de barras
    figure = go.Figure()

    # Adicionar barras para a variação de incêndios
    figure.add_trace(go.Bar(x=filtered_df['year'], y=filtered_df['Incendios'], name='Incêndios',marker_color='firebrick'))

    # Adicionar linha para a precipitação
    figure.add_trace(go.Scatter(x=precipitacao_municipio['year'], y=precipitacao_municipio['Precipitacao'],
                                mode='lines', name='Precipitação',line=dict(color='blue')))
    # Definir o layout do gráfico
    figure.update_layout(
        title=f'Variação de Incêndios e Precipitação em {selected_municipio} de 2013 / 2023',
        title_x=0.5,
        xaxis=dict(title='Ano'),
        yaxis=dict(title='Incêndios / Precipitação'),
    )

    # Exibir o gráfico
    return figure

@app.callback(
    Output('fogo_mensal', 'figure'),
    [Input("municipality", "value"),
     Input("years", "value")],prevent_initial_call=True)

def fire_versus_precipitation_monthly(municipio,year):
    if municipio is not None and year is not None:
        selected_row = mergedData[((mergedData["Municipio"]==municipio) & (mergedData["year"]== year))]
        dados_agrupados = selected_row.groupby(["Municipio", "month"]).agg({"Incendios":"sum","Precipitacao":"sum"}).reset_index()
        
        fig = go.Figure()
        fig.add_trace(go.Bar(x=dados_agrupados['month'], 
                             y=dados_agrupados['Incendios'], 
                             name='Incêndios',marker_color='firebrick'))
        fig.add_trace(go.Bar(x=dados_agrupados['month'], 
                             y=dados_agrupados['Precipitacao'], 
                             name='Precipitação',marker_color='blue'))

        # Definir o layout do gráfico
        fig.update_layout(
            title=f'Variação de Incêndios e Precipitação mensais em {municipio} no ano de {year}.',
            title_x=0.5,
            xaxis=dict(title='Mês'),
            yaxis=dict(title='Incêndios / Precipitação'),
        )

        return fig

    else:
        return {}

@app.callback(
    Output('teste', 'children'),
    [Input("municipality", "value"),
     Input("years", "value")],prevent_initial_call=True)

def fire_spots(municipio, year):
    if municipio is not None and year is not None:
        selected_row = mergedData[((mergedData["Municipio"]==municipio) & (mergedData["year"]== year))]

        
        fire_map = generate_map(municipio,selected_row)
       

        mc = MarkerCluster()
        for index, focosIncendio in selected_row.iterrows():
            mc.add_child(folium.Marker([focosIncendio['Latitude'], focosIncendio['Longitude']],
                    popup=str(f"Município:{focosIncendio['Municipio']}\nBioma:{focosIncendio['Bioma']}"),
                    tooltip=f"Município:{focosIncendio['Municipio']}\nBioma:{focosIncendio['Bioma']}",
                    icon=folium.Icon(color="red",icon='fire'))).add_to(fire_map)
        fire_map.save("maps/fireMap.html")

        return [html.H3('Distribuição de Incêndios'),html.Iframe(id="iframe2",srcDoc = open('maps/fireMap.html', 'r').read(),style={"border":"none","width":"100%","height":"600px"})]
    else:
        return {}
    

@app.callback(
    Output('heatMap', 'children'),
    [Input("municipality", "value"),
     Input("years", "value")],prevent_initial_call=True)

def heatMap(municipio, year):
    if municipio is not None and year is not None:
        selected_row = mergedData[((mergedData["Municipio"]==municipio) & (mergedData["year"]== year))]
        heatMap = generate_map(municipio,selected_row)
        heat_data = [[row['Latitude'],row['Longitude']] for index, row in selected_row.iterrows()]
        HeatMap(heat_data).add_to(heatMap)
        heatMap.save('maps/heat_map.html')
        

        return [html.H3('Mapa de calor'),html.Iframe(id="heatMap_id",srcDoc = open('maps/heat_map.html', 'r').read(),style={"border":"none","width":"100%","height":"600px"})]
    else:
        return {}
    

# Executar o servidor
if __name__ == '__main__':
    app.run_server(debug=True)