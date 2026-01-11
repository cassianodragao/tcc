import sys
import ast
import numpy as np
import time
import polars as pl
import pandas as pd
import xmltodict
import networkx as nx
from geopy.distance import geodesic


CENARIO_0 = sys.argv[1]
CENARIO_1 = sys.argv[2]
NUMERO = sys.argv[3]
CORES = [sys.argv[4]]

EVENTS = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/events_parquet/events_{CENARIO_0}_{NUMERO}.parquet',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/events_parquet/events_{CENARIO_1}_{NUMERO}.parquet'
]
BOARDING_TIMES = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/timelines/boarding_times_{NUMERO}.csv',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/timelines/boarding_times_{NUMERO}.csv'
]
board0, board1 = pd.read_csv(BOARDING_TIMES[0]), pd.read_csv(BOARDING_TIMES[1])

TRIPS = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/info/trips_{CENARIO_0}.xml',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/info/trips_{CENARIO_1}.xml'
]
TRIPS_DF = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/info/trips_df.csv',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/info/trips_df.csv'
]
ONIBUS_PEGADOS = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/onibus_pegados/onibus_pegados_{NUMERO}.csv',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/onibus_pegados/onibus_pegados_{NUMERO}.csv'
]
METROS = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/info/metro_{CENARIO_0}.csv',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/info/metro_{CENARIO_1}.csv'
]
NODES = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/info/nodes_com_metro.csv',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/info/nodes_com_metro.csv'
]
nodes0, nodes1 = pd.read_csv(NODES[0], sep=';'), pd.read_csv(NODES[1], sep=';')
EDGES = [
    f'/home/cassianodragao/scripts/dados/{CENARIO_0}/info/edges_com_metro.csv',
    f'/home/cassianodragao/scripts/dados/{CENARIO_1}/info/edges_com_metro.csv'
]
edges0, edges1 = pd.read_csv(EDGES[0], sep=';'), pd.read_csv(EDGES[1], sep=';')

ONIBUS = '/home/cassianodragao/scripts/dados/_GERAL/bus_corrigido.csv'

SAIDAS = [
    f'/home/cassianodragao/scripts/analise/{CENARIO_0}_{CENARIO_1}/{CENARIO_0}_pts_proximos_{NUMERO}.csv',
    f'/home/cassianodragao/scripts/analise/{CENARIO_0}_{CENARIO_1}/{CENARIO_1}_pts_proximos_{NUMERO}.csv'
]
############################################################################################
# olhar a linha(s) analisada(s), pegar os pontos proximos

# para isso, pegar as estacoes de metro das linhas em questao
metro = pd.read_csv(METROS[0])
metro['line'] = [ast.literal_eval(s) for s in metro['line']]
metro['neigh'] = [ast.literal_eval(s) for s in metro['neigh']]
estacoes_interesse = []
estacoes_interesse_id = []
for _, row in metro.iterrows():
    if any([cor in row['line'] for cor in CORES]):
        estacoes_interesse.append((row['lat'], row['lon']))
        id_station = nodes1.loc[(nodes1['lat']==row['lat'])&(nodes1['lon']==row['lon'])].iloc[0]['id']
        estacoes_interesse_id.append(id_station)
# somente os nodes que sao pontos de onibus
bus0 = nodes0.loc[nodes0['traffic_signals']==1]
bus1 = nodes1.loc[nodes1['traffic_signals']==1]
# para cada estacao, pegar os pontos de onibus proximos
pts_prox_0 = []
pts_prox_1 = []
for station_pt in estacoes_interesse:
    bus0['distance_station'] = bus0.apply(lambda row: geodesic(station_pt, (row['lat'], row['lon'])).meters, axis=1)
    bus1['distance_station'] = bus1.apply(lambda row: geodesic(station_pt, (row['lat'], row['lon'])).meters, axis=1)
    prox0 = list(bus0[bus0['distance_station']<=400]['id'])
    prox1 = list(bus1[bus1['distance_station']<=400]['id'])
    for p in prox0:
        if p not in pts_prox_0:
            pts_prox_0.append(p)
    for p in prox1:
        if p not in pts_prox_1:
            pts_prox_1.append(p)
# com as listas dos pontos proximos das estacoes, podemos olhar os links que levam para esses pontos,
# pois sao os links que aparecem na analise
links_paradas_0 = []
links_paradas_1 = []
for pt in pts_prox_0:
    links_to_pt = edges0.loc[edges0['to_']==pt]
    for l in list(links_to_pt['id']):
        links_paradas_0.append(l)
    # alem desses, adicionar tb os links q levam para o 'from_' desses
for pt in pts_prox_1:
    links_to_pt = edges1.loc[edges1['to_']==pt]
    for l in list(links_to_pt['id']):
        links_paradas_1.append(l)
# boarding times para os pontos proximos as linhas
b0 = board0.loc[board0['link_chegou_ponto'].isin(links_paradas_0)]
b0.to_csv(SAIDAS[0], index=False)
b1 = board1.loc[board0['link_chegou_ponto'].isin(links_paradas_1)]
b1.to_csv(SAIDAS[1], index=False)



