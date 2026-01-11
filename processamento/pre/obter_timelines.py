"""
"""
import sys
import numpy as np
import time
import polars as pl
import pandas as pd
import xmltodict
from functools import reduce
import operator
pl.Config.set_tbl_rows(110000)
#

CENARIO = sys.argv[1]
NUMERO  = sys.argv[2]

EV_PARQUET          = f'/home/cassianodragao/scripts/dados/{CENARIO}/events_parquet/events_{CENARIO}_{NUMERO}.parquet'
EV_CSV              = f'/home/cassianodragao/scripts/dados/{CENARIO}/events_csv/events_{CENARIO}_{NUMERO}.csv'
TRIPSFILE           = f'/home/cassianodragao/scripts/dados/{CENARIO}/info/trips_{CENARIO}.xml'
EDGESFILE           = f'/home/cassianodragao/scripts/dados/{CENARIO}/info/edges_com_metro.csv'
BUGADAS_FILE        = f'/home/cassianodragao/scripts/dados/_GERAL/VIAGENS_BUGADAS.txt'
ORI_DEST_FILE       = f'/home/cassianodragao/scripts/dados/_GERAL/ORIG_IGUAL_DEST_{CENARIO}.txt'
ONIBUS_PEGADOS_FILE = f'/home/cassianodragao/scripts/dados/{CENARIO}/onibus_pegados/onibus_pegados_{NUMERO}.csv'
BUS_XML_FILE        = f'/home/cassianodragao/scripts/dados/{CENARIO}/info/bus_integrado.xml'
AGENTES_MORTOS_FILE = f'/home/cassianodragao/scripts/dados/{CENARIO}/agentes_mortos/agentes_mortos_{NUMERO}.txt'
open(AGENTES_MORTOS_FILE, 'w').close()

SAIDA_FILE = f'/home/cassianodragao/scripts/dados/{CENARIO}/timelines/boarding_times_{NUMERO}.csv'

#events = 'dados/events_sp_mini.parquet'
df = pl.scan_parquet(EV_PARQUET)
DF_EVENTS = df.with_row_index()
DF_EVENTS_ = DF_EVENTS.collect()
##print(DF_EVENTS_)
#"""
index_agente_link = {
    (str(agt), int(link)): i
    for i, (agt, link) in enumerate(zip(DF_EVENTS_["agent"], DF_EVENTS_["link"]))
}
#"""
#
#tripsfile = 'dados/trips-teste-10-novo.xml'
with open(TRIPSFILE, 'r') as file:
    trips = xmltodict.parse(file.read())
multi = trips['scsimulator_matrix']['multi_trip']
multi_dict = {m['@name']: m for m in multi}
single = trips['scsimulator_matrix']['trip']
#
#edges_file = 'dados/edges_com_metro.csv'
edges = pd.read_csv(EDGESFILE, sep=';', low_memory=False)
edges_dict = {}
for _, row in edges.iterrows():
    edges_dict[row['id']] = row
dict_edges = {}
for _, edge in edges.iterrows():
    dict_edges[edge['id']] = edge.to_dict()
#
with open(BUGADAS_FILE, 'r') as f:
    bugadas = [i.rstrip('\n') for i in list(f.readlines())]
with open(ORI_DEST_FILE, 'r') as f:
    igual = [i.rstrip('\n') for i in list(f.readlines())]
NAO_OLHAR = bugadas + igual
#
ONIBUS_PEGADOS = pd.read_csv(ONIBUS_PEGADOS_FILE, sep=';')

with open(BUS_XML_FILE, 'r') as f:
    bus_xml = xmltodict.parse(f.read())
BUS_XML = {b['@id']: b['@stops'] for b in bus_xml['scsimulator_buses']['bus']}
##print(BUS_XML)
##########################################################################################################
def bustrip_stuff(trip_name, exemplo=1):
    # queremos devolver: arestas que o agente chega no ponto
    # queremos devolver: arestas que os onibus coletam agentes
    # queremos devolver: arestas que os onibus depositam os agentes
    exemplo_trip = DF_EVENTS.filter((pl.col('agent')==f'{trip_name}_{exemplo}')).collect().to_pandas().reset_index(drop=True)
    ##print(exemplo_trip)
    arestas_chegou_ponto = []
    arestas_desceu_bus = []
    for idx, row in exemplo_trip.iterrows():
        if row['movetype'] == 'move_bus':
            arestas_chegou_ponto.append(anterior['link'])
            arestas_desceu_bus.append(row['link'])
        anterior = row
    # agora as arestas que o onibus passa e coleta as peoples
    linhas_trip = [s for s in multi_dict[trip_name]['trip'] if s['@mode']=='bus']
    arestas_coletou_ponto = []
    for aresta_ponto, linha in zip(arestas_chegou_ponto, linhas_trip):
        trip_linha = DF_EVENTS.filter(pl.col('agent')==f'{linha["@line"]}1').collect().to_pandas().reset_index(drop=True)
        ##print(trip_linha)
        links_linha = list(trip_linha['link'][1: -1])
        #
        achou = False
        info_ponto = edges_dict[aresta_ponto]
        ##print(info_ponto)
        for l in links_linha:
            info_l = edges_dict[l]
            if ((info_ponto['to_'] == info_l['from_']) or
                (info_ponto['from_'] == info_l['from_']) or
                (info_ponto['from_'] == info_l['to_']) or
                (info_ponto['to_'] == info_l['to_'])
            ):
                arestas_coletou_ponto.append(info_l['id'])
                achou = True
                break
                ##print('ok')
            ##print(info_l['from_'], info_l['to_'], info_l['id'])
        if not achou:
            # pode ser:
            # (1) primeiro ponto do onibus (saida vem diferente)
            terminal =  BUS_XML[linha['@line']].split(',')[0]
            ini_subtrip = linha['@origin']
            if terminal == ini_subtrip:
                arestas_coletou_ponto.append(1)
                achou = True
                break
        if not achou:
            # tentar achar um link com o mesmo 'from' do link do ponto, mas que
            # leve ao node q tem o link q o onibus passa
            ini_subtrip = int(linha['@origin'])
            links_from_ponto = edges.loc[edges['from_']==info_ponto['from_']]
            for _, lfp in links_from_ponto.iterrows():
                if (lfp['to_'] == ini_subtrip) and (str(lfp['to_']) in BUS_XML[linha['@line']].split(',')):
                    ##print(lfp)
                    # olhar as arestas que partem desse^ ponto (ini_subtrip), a que tiver no caminho do onibus eh a certa
                    links_from_ponto_2 = edges.loc[edges['from_']==lfp['to_']]
                    for _, lfp2 in links_from_ponto_2.iterrows():
                        if lfp2['id'] in links_linha:
                            arestas_coletou_ponto.append(lfp2['id'])
                            achou = True
                            break
                ##print('ok v2')
                if achou:
                    break
    ##print(exemplo_trip)
    ##print(linhas_trip)
    try:
        if not achou:
            raise Exception(f'Nao consegui identificar onde o onibus pega o passageiro na trip:  {trip_name}')
    except UnboundLocalError:
        return bustrip_stuff(trip_name, exemplo+1)
    
    # retorna 2 listas:
    # uma contendo tuplas com as areastas (agentes chegam ponto, onibus chegam ponto)
    # outra contendo as arestas agentes descem onibus
    return [(chegou, coletou) for chegou, coletou in zip(arestas_chegou_ponto, arestas_coletou_ponto)], arestas_desceu_bus
##########################################################################################################
# retornar:
# 1 linha por onibus pegado pelo agente
# agente ; time chegou ponto; link ; time pegou onibus ; id onibus ; time desceu onibus; link
def le_trip(trip_name):
    if trip_name in NAO_OLHAR: return
    onibus_pegados_trip = ONIBUS_PEGADOS.loc[ONIBUS_PEGADOS['trip']==trip_name]
    if onibus_pegados_trip.empty:
        #trip nao usa onibus
        return
    ##print(onibus_pegados_trip)
    ##print(onibus_pegados_trip)
    trip_info = multi_dict[trip_name]
    links_important, arestas_desceu = bustrip_stuff(trip_name)
    ##print(links_important)
    ##print(trip_info)
    ##print(links_important)
    data_return = []
    colunas = [
        'agente', 'time_chegou_ponto', 'link_chegou_ponto',
        'time_pegou_onibus', 'onibus_pegado', 'time_desceu_onibus', 'link_desceu_onibus'
    ]
    # primeiro olhar as horas que os onibus passaram no ponto, pq é igual pra muitos
    linhas = [s['@line'] for s in trip_info['trip'] if s['@mode']=='bus']
    bus_coletou_ponto = {}  # id_bus, hora q coletou
    for linha, arestas, aresta_desceu in zip(linhas, links_important, arestas_desceu) :
        buses_linha = list(onibus_pegados_trip.loc[onibus_pegados_trip['pegou'].str.startswith(linha)]['pegou'])
        buses_diferentes = np.unique(buses_linha)
        
        ##print(buses_diferentes)
        # dict usado para ver facilmente (computacionalmente facil) qual bus cada agente pegou
        qual_bus_dif_agente = {}
        momento_bus_dif_deposita_agentes = {}
        aresta_bus_dif_deposita_agentes = {}
        for bus_dif in buses_diferentes:
            idx = index_agente_link[(bus_dif, arestas[1])]
            bus_coletou_ponto[bus_dif] = DF_EVENTS_[idx]['time'][0]
            aux = onibus_pegados_trip.loc[onibus_pegados_trip['pegou']==bus_dif]
            
            for _, row in aux.iterrows():
                qual_bus_dif_agente[row['agente']] = row['pegou']
                exemplo = row['agente']
            
            idx_exemplo_desce = index_agente_link[(exemplo, aresta_desceu)]
            # todos que pegaram esse busdif descem juntos no msm segundo
            aux2 = DF_EVENTS_[idx_exemplo_desce]
            time_exemplo_desce = aux2['time'][0]
            momento_bus_dif_deposita_agentes[bus_dif] = time_exemplo_desce
            aresta_exemplo_desce = aux2['link'][0]
            # na verdade eh a aresta q o agente passou dps de descer do onibus
            aresta_bus_dif_deposita_agentes[bus_dif] = aresta_exemplo_desce
            
        ##print(bus_coletou_ponto)
        # agora olhar os momentos que cada agente chegou no ponto
        for agente in range(1, int(trip_info['@count'])+1):
            agt_name = f'{trip_name}_{agente}'
            try:
                idx = index_agente_link[(agt_name, arestas[0])]
                time_chegou_ponto = DF_EVENTS_[idx]['time'][0]
                link_chegou_ponto = arestas[0]
                
                qual_onibus = qual_bus_dif_agente[agt_name]
                time_pegou_onibus = bus_coletou_ponto[qual_onibus]
                
                time_desceu_bus = momento_bus_dif_deposita_agentes[qual_onibus]
                link_desceu_ponto = aresta_bus_dif_deposita_agentes[qual_onibus]
                data_return.append([
                    agt_name,
                    time_chegou_ponto,
                    link_chegou_ponto,
                    time_pegou_onibus,
                    qual_onibus,
                    time_desceu_bus,
                    link_desceu_ponto
                ])
            except:
                with open(AGENTES_MORTOS_FILE, 'a') as m:
                    m.write(agt_name+'\n')
                continue
    
    return data_return
            ##print(f'{trip_name}_{agente}', time_chegou_ponto)
            
import csv
ini = time.time()  
colunas = [
    'agente', 'time_chegou_ponto', 'link_chegou_ponto',
    'time_pegou_onibus', 'onibus_pegado', 'time_desceu_onibus', 'link_desceu_onibus'
    ]  
saida = open(SAIDA_FILE, 'w', newline='', encoding='utf-8')
writer = csv.writer(saida)
writer.writerow(colunas)
for trip in list(multi_dict.keys()):
    #print(trip)
    dados = le_trip(trip)
    if dados:
        writer.writerows(dados)
    #print(time.time() - ini)
saida.close()
exit()

if 1:
    ini = time.time()
    le_trip('trip_139')
    #print(time.time() - ini)
    time.sleep(1000)


trip_179 = DF_EVENTS.filter(
    (pl.col('agent')=='trip_139_1')|(pl.col('agent')=='271A-10-043')
).collect()
#print(trip_179)
