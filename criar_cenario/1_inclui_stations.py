"""
ESSE SCRIPT AINDA ESTA SENDO ADAPTADO PARA NOVOS CENARIOS

ELE APENAS PEGA AS ESTACOES DO CSV E INSERE ELAS NOS NODES DO BAGULHO. ELE NAO CRIA O ARQUIVO DE METROS
"""
import ast
import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.spatial import KDTree

CENARIO = 'prata_1000'
# entrada eh sempre 'nodes_sem_metro' e 'edges_sem_metro'
CSV_METRO = F'metro_{CENARIO}'
SAIDA = 'com_metro'


# lendo o csv de metro basico
metro = pd.read_csv(f'cenarios/{CENARIO}/{CSV_METRO}.csv')
metro['line'] = [ast.literal_eval(s) for s in metro['line']]
metro['neigh'] = [ast.literal_eval(s) for s in metro['neigh']]
metro_gdf = gpd.GeoDataFrame(
    metro,
    geometry=gpd.points_from_xy(metro.lon, metro.lat),
    crs='EPSG:4326'
)
metro_proj = metro_gdf.to_crs(epsg=3857)  # metro com a coluna 'geometry'
print(metro_proj)


# NODES E EDGES
nodes = pd.read_csv(f'cenarios/{CENARIO}/nodes_sem_metro.csv', sep=';')
nodes_gdf = gpd.GeoDataFrame(
    nodes,
    geometry=gpd.points_from_xy(nodes.lon, nodes.lat),
    crs='EPSG:4326'
)
nodes_proj = nodes_gdf.to_crs(epsg=3857)  # nodes com 'geometry'
edges = pd.read_csv(f'cenarios/{CENARIO}/edges_sem_metro.csv', sep=';')
next_node_id = max(nodes_proj['id']) + 1
next_edge_id = max(edges['id']) + 1

# tree que facilita a busca
node_crs = list(zip(nodes_proj.geometry.x, nodes_proj.geometry.y))
kdtree = KDTree(node_crs)
# para cada estacao de metro, criar seu nó e liga-lo ao grafo (edge de ida e volta)
nodes_add = []
edges_add = []
for idx, row in metro_proj.iterrows():
    new_node = [next_node_id, row['lat'], row['lon'], -1]
    nodes_add.append(new_node)
    
    metro_point = (row.geometry.x, row.geometry.y)
    length, index = kdtree.query(metro_point)
    node_proximo = nodes_proj.iloc[index]
    #id;from_;to_;length;freespeed;permlanes;oneway;modes;capacity;geometry
    new_edge = [
        next_edge_id, node_proximo['id'], next_node_id, length, 1.45*3.6, 1, 1, 'walk', 6000.0, 'metro'
    ]
    edges_add.append(new_edge)
    next_edge_id += 1
    new_edge = [
        next_edge_id, next_node_id, node_proximo['id'], length, 1.45*3.6, 1, 1, 'walk', 6000.0, 'metro' 
    ]
    edges_add.append(new_edge)
    next_edge_id += 1
    
    next_node_id += 1

edges_novos_df = pd.DataFrame(edges_add, columns=edges.columns)
edges = pd.concat([edges, edges_novos_df], ignore_index=True)

nodes_novos_df = pd.DataFrame(nodes_add, columns=nodes.columns)
nodes = pd.concat([nodes, nodes_novos_df], ignore_index=True)

nodes['id'] = nodes['id'].astype('int64')
nodes['traffic_signals'] = nodes['traffic_signals'].astype('int64')
nodes.to_csv(f'cenarios/{CENARIO}/nodes_{SAIDA}.csv', sep=';', index=False)

edges['from_'] = edges['from_'].astype('int64')
edges['to_'] = edges['to_'].astype('int64')
edges.to_csv(f'cenarios/{CENARIO}/edges_{SAIDA}.csv', sep=';', index=False)