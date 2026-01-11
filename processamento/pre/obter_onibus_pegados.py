"""
script usado para definir algumas funcoes uteis e para criar o csv dos onibus pegados.

trip_3806 -> trip com bus + metro
"""
import sys
import time
import polars as pl
import pandas as pd
import xmltodict
from functools import reduce
import operator
pl.Config.set_tbl_rows(110000)

CENARIO = sys.argv[1]
NUMERO  = sys.argv[2]


EV_PARQUET = f'/home/cassianodragao/scripts/dados/{CENARIO}/events_parquet/events_{CENARIO}_{NUMERO}.parquet'                                                                                                  #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/saida/events_{CENARIO}.parquet'
EV_CSV     = f'/home/cassianodragao/scripts/dados/{CENARIO}/events_csv/events_{CENARIO}_{NUMERO}.csv'                                                                                                      #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/saida/events_{CENARIO}.csv'
TRIPSFILE  = f'/home/cassianodragao/scripts/dados/{CENARIO}/info/trips_{CENARIO}.xml'                                                                                                         #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/trips_{CENARIO}.xml'
EDGESFILE  = f'/home/cassianodragao/scripts/dados/{CENARIO}/info/edges_com_metro.csv'                                                                                                                             #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/edges_com_metro.csv'

SAIDA_ONIBUS_PEGADOS = f'/home/cassianodragao/scripts/dados/{CENARIO}/onibus_pegados/onibus_pegados_{NUMERO}.csv'                                                                                                                                                     #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/saida/onibus_pegados.csv'

BUGADAS_FILE    = f'/home/cassianodragao/scripts/dados/_GERAL/VIAGENS_BUGADAS.txt'
ORIG_IGUAL_DEST = f'/home/cassianodragao/scripts/dados/_GERAL/ORIG_IGUAL_DEST_{CENARIO}.txt'
if NUMERO == '0':
    open(ORIG_IGUAL_DEST, 'w').close()



#events = 'dados/events_sp_mini.parquet'
df = pl.scan_parquet(EV_PARQUET)
df = df.with_row_index()

#tripsfile = 'dados/trips-teste-10-novo.xml'
with open(TRIPSFILE, 'r') as file:
    trips = xmltodict.parse(file.read())
multi = trips['scsimulator_matrix']['multi_trip']
multi_dict = {m['@name']: m for m in multi}
single = trips['scsimulator_matrix']['trip']

#edges_file = 'dados/edges_com_metro.csv'
edges = pd.read_csv(EDGESFILE, sep=';')
dict_edges = {}
for _, edge in edges.iterrows():
    dict_edges[edge['id']] = edge.to_dict()

with open(BUGADAS_FILE, 'r') as f:
    bugadas = [i.rstrip('\n') for i in list(f.readlines())]

#############################################################################################################
# funcao que filtra os events relacionados a uma trip que envolve onibus
def read_bustrip(trip_name):
    if trip_name in bugadas:
        return None
    info_trip = multi_dict[trip_name]
    buses_relevantes = []
    pontos_inicio = []
    for subtrip in info_trip['trip']:
        if subtrip['@mode'] == 'bus':
            if subtrip['@origin'] == subtrip['@destination']:
                # anotar esses casos problematicos q nem sei quantos tem
                if NUMERO == '0':
                    with open(ORIG_IGUAL_DEST, 'a') as f:
                        f.write(trip_name+'\n')
                return
            buses_relevantes.append(subtrip['@line'])
            pontos_inicio.append(subtrip['@origin'])
    if buses_relevantes == []:
        ##print('nao é viagem de bus')
        return None
    ##print(info_trip)
    # filtrar os eventos para conter somente as coisas relevantes a essa trip
    substrings = buses_relevantes + [f'{trip_name}_']
    filtro = reduce(operator.or_, [pl.col('agent').str.contains(s) for s in substrings])
    subevents = df.filter(filtro)
    ##print(subevents.collect())
    # quebrar os eventos quando ocorre o 'arrival' da ultima trip desse tipo
    # (depois disso so tem onibus vazio[com outras trips])
    try:
        just_trip_arrival = df.filter(
            (pl.col('agent').str.contains(f'{trip_name}_'))&(pl.col('movetype')=='arrival')
        )
        latest = just_trip_arrival.select(pl.col('time').max()).collect()['time'].item() + 120
        # tbm quebrar o inicio. vamos olhar somente ate 1h antes no inicio da primeira trip
        just_trip_start = df.filter(
            (pl.col('agent').str.contains(f'{trip_name}_'))&(pl.col('movetype')=='start')
        )
        earliest = just_trip_start.select(pl.col('time').min()).collect()['time'].item() - 3600
        # o subevents eh tudo q importa dessa trip
        subevents = subevents.filter(
            (pl.col('time') >= earliest)&(pl.col('time') <= latest)
        )
    except TypeError:
        # se entrar aqui eh pq a trip nao tem 'arrival' ou 'start'
        with open(BUGADAS_FILE, 'a') as bugfile:
            bugfile.write(f'{trip_name}\n')
        return None
    ##print(subevents.collect())
    pegados = qual_bus_pegaram(subevents)
    return pegados
    
    
    """
    timelines = []  # agente, chegou_ponto, id_ponto, pegou_onibus, id_onibus
    vip_links = bustrip_stuff(info_trip, subevents)
    # para cada onibus, olhar os agentes da trip, e calcular ^
    for links, bus_relevante, pt_inicio in zip(vip_links, buses_relevantes, pontos_inicio):
        events_bus = subevents.filter(pl.col('agent').str.starts_with(bus_relevante))
        
        
        
        for agente in range(int(info_trip['@count'])):
            agente_chegou_ponto = subevents.filter(
                (pl.col('agent')==f'{trip_name}_{agente+1}')&(pl.col('link')==links[0])
            )
            ##print(agente_chegou_ponto.collect())
            # esse agente pegou o proximo onibus a passar pelo link ^ (nem sempre zzzz)
            chegou_time = agente_chegou_ponto.collect()['time'].item()
            
            
            
            
            
            
            proximos = events_bus.filter(
                (pl.col('time')>chegou_time)&(pl.col('link')==links[1])
            ).collect()
            #print(f'{trip_name}_{agente+1}')
            #print(proximos)
            bus_pegado = proximos.row(0, named=True)
            
            tl_agente = [
                f'{trip_name}_{agente+1}', chegou_time, int(pt_inicio), bus_pegado['time'], bus_pegado['agent']
            ]
            timelines.append(tl_agente)
    #print(subevents.collect())
    return timelines
    """
#############################################################################################################
# funcao que devolve uma lista com as arestas que uma linha de onibus passa
def links_bus(busline):
    busline_trip = df.filter((pl.col('agent')==f'{busline}1')).collect()
    return list(busline_trip['link'])[1: -1]
#############################################################################################################
# funcao que determina o momento em que um agente chegou no ponto, e qual onibus ele pegou, dado um events
def real_timeline(agent_name, events):  # events é um df.collect() da funcao read_bustrip
    pass
#############################################################################################################
def bustrip_stuff(info_trip, subevents):
    # dado uma trip, devolve quais pontos devemos dar atencao
    # [aresta que o agente anda e chega no ponto   ;
    #  aresta que o onibus coleta agentes no ponto ;
    #  aresta que o onibus devolve os passageiross ;   ]
    trip_1 = subevents.filter((pl.col('agent')==f'{info_trip["@name"]}_1')).collect()
    #print(trip_1)
    arestas = []  # [ [chegou_ponto, coletar_ponto], [chegou_ponto, ...]  ]
    for subtrip in info_trip['trip']:
        if subtrip['@mode'] == 'bus':
            caminho = links_bus(subtrip['@line']) 
            for a in caminho:
                edge = edges.loc[edges['id']==int(a)].iloc[0]
                if edge['to_'] == int(subtrip['@destination']):
                    edge_devolve = int(a)
                
            for row in trip_1.iter_rows(named=True):
                l = int(row['link'])
                info_link = edges.loc[edges['id']==l].iloc[0]
                if str(info_link['from_']) == subtrip['@origin']:
                    link_chegou_ponto = int(info_link['id'])
                    link_coleta_ponto = int(subtrip['@link_origin'])
                    arestas.append([link_chegou_ponto, link_coleta_ponto, edge_devolve])
    return arestas  
#############################################################################################################
# pega um events, devolve um dicionario
# {agente: [pegou esse onibus, pegou esse onibus, ...]}
# nao indica quando abordou o onibus, apenas que pegou
def qual_bus_pegaram(subevents):
    saida = {}
    ##print(subevents.collect())
    move_bus = subevents.filter(pl.col('movetype')=='move_bus')
    times = list(move_bus.select(pl.col("time").unique()).collect()['time'])
    ##print(times)
    for t in times:
        # events daquele momento, onde só tem gente descendo do onibus, ou onibus andando
        events_time = subevents.filter(
            ((pl.col('time')<=t+1)&(pl.col('time')>=t-1))&
            ((pl.col('movetype')=='move_bus')|~(pl.col('agent').str.starts_with('trip')))
        
        )
        ##print(events_time.collect())
        # se isso de alguma forma pegar 2 onibus diferentes (trip com 3 onibus eh teoricamente possivel), da erro
        teste = list(events_time.filter(pl.col('movetype')=='move_bus').select(pl.col("time").unique()).collect()['time'])
        if len(teste) > 1: 
            events_time = subevents.filter(
                (pl.col('time')==t)&
                ((pl.col('movetype')=='move_bus')|~(pl.col('agent').str.starts_with('trip')))
            )
            teste = list(events_time.filter(pl.col('movetype')=='move_bus').select(pl.col("time").unique()).collect()['time'])
            if len(teste) > 1:
                raise Exception('fudeuuu')
        
        achou = False
        #events_time = events_time.collect()
        # olhar o 'from_' das edges dos 'move_bus', se bater com o 'to_' de algum onibus, eh isso
        #passageiros = events_time.filter(pl.col("movetype") == "move_bus")    #events_time.loc[events_time['movetype']=='move_bus']
        onibus = events_time.filter((pl.col("movetype") == "move")|(pl.col("movetype") == "arrival")).collect()
        #link_pass = passageiros['link'][0]
        link_pass = (
            events_time
            .filter(pl.col("movetype") == "move_bus")
            .select("link")
            .limit(1)         # <= only one row
            .collect()        # triggers execution for just 1 row
            .item()           # extract scalar
        )
        info_link_pass = dict_edges[link_pass]
        pass_from = info_link_pass['from_']
        pass_to = info_link_pass['to_']
        ##print(events_time)
        for bus in onibus.iter_rows(named=True):
            link_bus = bus['link']
            info_link_bus = dict_edges[link_bus]
            if (info_link_bus['from_'] == pass_from
                or info_link_bus['to_'] == pass_from
                or info_link_bus['to_'] == pass_to
                or info_link_bus['from_'] == pass_to
            ):
                achou = True
                passageiros = events_time.filter(pl.col("movetype") == "move_bus").collect()
                # todos esses caras ai pegaram o onibus 'bus'
                ##print(passageiros)
                bus_catched = bus['agent']
                for trip_person in list(passageiros['agent']):
                    try:
                        saida[trip_person].append(bus_catched)
                    except:
                        saida[trip_person] = [bus_catched]
    return saida
#############################################################################################################   


# area de testes
#tls = read_bustrip('trip_8927')
#tls = read_bustrip('trip_16384')


colunas_csv = ['trip', 'agente', 'pegou', 'seq']
dados = []
ini = time.time()
for mu in multi:
    m = mu['@name']
    #print(m)
    #print(time.time() - ini)
    info = read_bustrip(m)
    if info is None:
        continue
    for agente in info.keys():
        for bus, idx in zip(info[agente], range(len(info[agente]))):
            dados.append([m, agente, bus, idx+1])

csv_infobus = pd.DataFrame(dados, columns=colunas_csv)
#csv_infobus.to_csv('dados/onibus_pegados.csv', sep=';', index=False)
csv_infobus.to_csv(SAIDA_ONIBUS_PEGADOS, sep=';', index=False)

      
