"""
"""
import ast
import pandas as pd
import xmltodict

CENARIO = 'prata_1000'
NODES = 'nodes_com_metro'
EDGES = 'edges_com_metro'
METRO = f'metro_{CENARIO}'

##########################################################################################################################
# lendo o csv com as estacoes em um pandas, vamos usar ele para adicionar novas estacoes mais facilmente
nodes = pd.read_csv(f'cenarios/{CENARIO}/{NODES}.csv', sep=';')
metro_nodes = nodes.loc[nodes['traffic_signals']==-1]
metro_csv = pd.read_csv(f'cenarios/{CENARIO}/{METRO}.csv')
metro_csv['line'] = [ast.literal_eval(s) for s in metro_csv['line']]
metro_csv['neigh'] = [ast.literal_eval(s) for s in metro_csv['neigh']]
dados_metro = []
colunas = ['id_station', 'nome_station']
for _, row in metro_nodes.iterrows():
    id_station = row['id']
    nome_station = metro_csv.loc[(metro_csv['lat']==row['lat'])&(metro_csv['lon']==row['lon'])].iloc[0]['name-fresco']
    dados_metro.append([id_station, nome_station])
df_stations = pd.DataFrame(dados_metro, columns=colunas)
edges_metro = []
colunas_edges = ['nome_ori', 'nome_dest', 'id_ori', 'id_dest']
for idx, row in metro_csv.iterrows():
    station_id = int(df_stations.loc[df_stations['nome_station']==row['name-fresco']].iloc[0]['id_station'])
    station_name = row['name-fresco']
    for estacao in row['neigh']:
        estacao_id = int(df_stations.loc[df_stations['nome_station']==estacao].iloc[0]['id_station'])
        
        edge = [station_name, estacao, station_id, estacao_id]
        if edge not in edges_metro:
            edges_metro.append(edge)
df_metro = pd.DataFrame(edges_metro, columns=colunas_edges)
print(df_metro)
#########################################################################################################################


dict_xml = {'metro': {'stations': {'station': []}, 'links': {'link': []}}}
for _, row in df_stations.iterrows():
    dict_xml['metro']['stations']['station'].append(
        {'@name': f'{row["nome_station"]}', '@idNode': f'{int(row["id_station"])}'}
    )
for _, row in df_metro.iterrows():
    dict_xml['metro']['links']['link'].append(
        {'@nameOrigin': f'{row["nome_ori"]}', '@nameDestination': f'{row["nome_dest"]}',
         '@idOrigin': f'{int(row["id_ori"])}', '@idDestination': f'{int(row["id_dest"])}'}
    )

with open(f'cenarios/{CENARIO}/metro_gerado.xml', 'w') as file:
    file.write(xmltodict.unparse(dict_xml, pretty=True, short_empty_elements=True))
