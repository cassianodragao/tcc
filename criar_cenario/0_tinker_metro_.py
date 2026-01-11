"""
"""
import ast
import numpy as np
import pandas as pd
import geopandas as gpd
from scipy.spatial import KDTree
from shapely.geometry import Point, LineString


CENARIO = 'prata_1000'
#ENTRADA = f'cenarios/{CENARIO}/metro_teste_cor.csv'
SAIDA = f'cenarios/{CENARIO}/metro_{CENARIO}.csv'

# lendo o csv de metro basico
metro = pd.read_csv(f'cenarios/base/metrosp_stations.csv')
#metro = pd.read_csv(ENTRADA)
metro['line'] = [ast.literal_eval(s) for s in metro['line']]
metro['neigh'] = [ast.literal_eval(s) for s in metro['neigh']]
metro_gdf = gpd.GeoDataFrame(
    metro,
    geometry=gpd.points_from_xy(metro.lon, metro.lat),
    crs='EPSG:4326'
)
METRO = metro_gdf.to_crs(epsg=3857)  # metro com a coluna 'geometry'
print(METRO)

##############################################################################################
# altera o csv, inserindo uma estacao exatamente no meio das duas passadas
def inserir_station(station_1, station_2):
    
    # primeiro verificar se estao realmente ligadas
    st1 = METRO.loc[METRO['name-fresco']==station_1]
    st2 = METRO.loc[METRO['name-fresco']==station_2]
    if not ((station_2 in st1.iloc[0]['neigh']) and (station_1 in st2.iloc[0]['neigh'])):
        raise Exception(f'{station_1} e {station_2} nao estao ligadas!')
    
    # pra inserir a estacao, pegar o ponto medio entre essas duas
    p1, p2 = st1.geometry.iloc[0], st2.geometry.iloc[0]
    midpoint = LineString([p1, p2]).interpolate(0.5, normalized=True)
    midpoint_latlon = gpd.GeoSeries([midpoint], crs=3857).to_crs(4326).iloc[0]
    #print(p1, p2, midpoint)
    
    # nome da estacao vai ser estacao-{numero}
    nomes_existentes = list(METRO['name-fresco'])
    estacoes_ficticias = [n for n in nomes_existentes if n.startswith('estacao_')]
    n = 0
    for st in estacoes_ficticias:
        n += 1
    station_name = f'estacao_{n}'
    
    # ligacoes da estacao nova sao as 2 antigas
    station_neigh = [station_1, station_2]
    # precisamos descobrir a cor da linha dessa estacao
    # a cor vai ser a cor que aparecer nas 2 ligadas
    st1_cores = list(METRO.loc[METRO['name-fresco']==station_1, 'line'])[0]
    st2_cores = list(METRO.loc[METRO['name-fresco']==station_2, 'line'])[0]
    print(st1_cores)
    print(st2_cores)
    for cor in st1_cores:
        if cor in st2_cores:
            line = [cor]
            break
    ############################################
    # agora vamos alterar as estacoes que ja estao no METRO
    st1_neigh = list(METRO.loc[METRO['name-fresco']==station_1, 'neigh'])[0]
    st2_neigh = list(METRO.loc[METRO['name-fresco']==station_2, 'neigh'])[0]
    st1_neigh.remove(station_2)
    st2_neigh.remove(station_1)
    
    mask = METRO['name-fresco'] == station_1
    row_index = METRO.index[mask][0]
    METRO.at[row_index, 'neigh'] = st1_neigh + [station_name]
    mask = METRO['name-fresco'] == station_2
    row_index = METRO.index[mask][0]
    METRO.at[row_index, 'neigh'] = st2_neigh + [station_name]
    
    # inserir a estacao nova
    station_row = [station_name, station_name, station_name, 
                   round(midpoint_latlon.y, 7), round(midpoint_latlon.x, 7), line, station_neigh, midpoint]
    METRO.loc[len(METRO)] = station_row

def set_max_dist_linha(line, max_dist=1000):
    # primeiro, separar todas estacoes dessa linha, vamos iterar por elas
    # vai iterando... se achar um par com maxdist maior, poe uma estacao no meio e recomeça
    while True:
        stations_linha = METRO[METRO['line'].apply(lambda x: line in x)]
        list_names = list(stations_linha['name-fresco'])
        recomecar = False
        for _, st in stations_linha.iterrows():
            #print(st['name-fresco'])
            geo_st = st['geometry']
            neigh = list(METRO.loc[METRO['name-fresco']==st['name-fresco'], 'neigh'])[0]
            # se a distancia ate uma neigh for grande, poe uma estacao no meio e recomeca TUDO
            for st_neigh in neigh:
                st_neigh_info = METRO[METRO['name-fresco']==st_neigh].iloc[0]
                # so entra nisso abaixo se for uma stacao de baldeacao, dai pula as estacoes da outra linha
                if line not in st_neigh_info['line']:
                    continue
                print(st['name-fresco'], st_neigh_info['name-fresco'])
                dist = st['geometry'].distance(st_neigh_info['geometry'])
                print(dist)
                if dist > max_dist:
                    inserir_station(st['name-fresco'], st_neigh_info['name-fresco'])
                    recomecar = True
                
                if recomecar:
                    break
            if recomecar:
                break
        if recomecar:
            continue
        # so vai chegar aqui se nao tiver encontrado nenhum par longe
        break

set_max_dist_linha('prata')
METRO.iloc[:, :-1].to_csv(SAIDA, index=False)
    
#inserir_station('luz', 'estacao_0')


#METRO.iloc[:, :-1].to_csv(SAIDA, index=False)