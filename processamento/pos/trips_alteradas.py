"""
Esse script vai comparar somente o cenario base com outros, nao os outros entre eles. (acho que nem vou fazewr isso)
"""
import sys
import ast
import numpy as np
import time
import polars as pl
import pandas as pd
import xmltodict
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

ONIBUS = '/home/cassianodragao/scripts/dados/_GERAL/bus_corrigido.csv'


SAIDA = f'/home/cassianodragao/scripts/analise/{CENARIO_0}_{CENARIO_1}/trips_alteradas_{NUMERO}.csv'
################################################################################################################
# primeiro, separar as linhas de onibus que passam proximas aos metros alterados

# para isso, pegar as estacoes de metro das linhas em questao
metro = pd.read_csv(METROS[1])
metro['line'] = [ast.literal_eval(s) for s in metro['line']]
metro['neigh'] = [ast.literal_eval(s) for s in metro['neigh']]
estacoes_interesse = []
estacoes_interesse_id = []
for _, row in metro.iterrows():
    if any([cor in row['line'] for cor in CORES]):
        estacoes_interesse.append((row['lat'], row['lon']))
        id_station = nodes1.loc[(nodes1['lat']==row['lat'])&(nodes1['lon']==row['lon'])].iloc[0]['id']
        estacoes_interesse_id.append(id_station)
# pegar as linhas de onibus que passam proximo as 'estacoes_interesse'
buses = pd.read_csv(ONIBUS, sep=';')
linhas = list(np.unique(list(buses['trip_id'])))
#"""
limit = 300 # distancia maxima do metro
linhas_interesse = []
for linha in linhas:
    print(linhas_interesse)
    olhar_linha = False
    pts_linha = buses.loc[buses['trip_id']==linha]
    for _, parada in pts_linha.iterrows():
        pt_parada = (parada['node_lat'], parada['node_lon'])
        for station in estacoes_interesse:
            if geodesic(pt_parada, station).meters <= limit:
                linhas_interesse.append(linha)
                olhar_linha = True
                break
        if olhar_linha:
            break
#"""
# caso lilas com 400m (118 linhas)
#linhas_interesse = ['175T-10-0', '175T-10-1', '4032-10-0', '4032-10-1', '407M-10-0', '4706-10-0', '4706-10-1', '4708-10-0', '4708-10-1', '4709-10-0', '4709-10-1', '4716-10-0', '4716-10-1', '4718-10-0', '4725-10-0', '475R-10-0', '475R-10-1', '476A-10-0', '476A-10-1', '476G-10-0', '476G-10-1', '476L-10-0', '477P-10-0', '477P-10-1', '477U-10-0', '5103-10-0', '5111-10-0', '5111-10-1', '5119-10-0', '5119-10-1', '5154-10-0', '5154-10-1', '5164-10-0', '5164-21-0', '5300-10-0', '5300-10-1', '5701-10-0', '5701-10-1', '6001-10-0', '6001-10-1', '6200-10-0', '6200-10-1', '637P-10-0', '637P-10-1', '6403-10-0', '6403-10-1', '6450-10-0', '6450-10-1', '6451-10-0', '6451-10-1', '6455-10-0', '6455-10-1', '648P-10-0', '648P-10-1', '6500-10-0', '6500-10-1', '669A-10-0', '669A-10-1', '675I-10-0', '675I-10-1', '675L-10-0', '675L-10-1', '6804-10-0', '6804-10-1', '6806-10-0', '6806-10-1', '6837-10-0', '6837-10-1', '695T-10-0', '695T-10-1', '695V-10-0', '695V-10-1', '7013-10-0', '7013-10-1', '709M-10-0', '709M-10-1', '709P-10-0', '709P-10-1', '7245-21-0', '736G-10-0', '736G-10-1', '736I-10-0', '736I-10-1', '746C-10-0', '746C-10-1', '746H-10-0', '746H-10-1', '746K-10-0', '746P-10-0', '746P-10-1', '746R-10-0', '8078-10-0', '807P-10-0', '875A-10-0', '875A-10-1', '875C-1-0', '875C-1-1', '875C-10-0', '875C-10-1', 'N505-11-0', 'N505-11-1', 'N506-11-0', 'N506-11-1', 'N604-11-0', 'N604-11-1', 'N633-11-0', 'N701-11-0', 'N701-11-1', 'N702-11-0', 'N702-11-1', 'N704-11-0', 'N704-11-1', 'N705-11-0', 'N705-11-1', 'N731-11-0', 'N731-11-1', 'N742-11-0', 'N742-11-1']
# caso lilas com 300m (114 linhas)
#linhas_interesse = ['175T-10-0', '175T-10-1', '4032-10-0', '4032-10-1', '407M-10-0', '4706-10-0', '4706-10-1', '4708-10-0', '4708-10-1', '4709-10-0', '4709-10-1', '4716-10-0', '4716-10-1', '4718-10-0', '4725-10-0', '475R-10-0', '475R-10-1', '476A-10-0', '476A-10-1', '476G-10-0', '476G-10-1', '476L-10-0', '477P-10-0', '477P-10-1', '477U-10-0', '5103-10-0', '5111-10-0', '5111-10-1', '5119-10-0', '5119-10-1', '5154-10-0', '5154-10-1', '5164-10-0', '5164-21-0', '5300-10-0', '5300-10-1', '5701-10-0', '5701-10-1', '6001-10-0', '6001-10-1', '6200-10-0', '6200-10-1', '637P-10-0', '637P-10-1', '6403-10-0', '6403-10-1', '6450-10-0', '6450-10-1', '6451-10-0', '6451-10-1', '6455-10-0', '6455-10-1', '648P-10-0', '648P-10-1', '6500-10-0', '6500-10-1', '669A-10-0', '669A-10-1', '675I-10-0', '675I-10-1', '675L-10-0', '675L-10-1', '6804-10-0', '6804-10-1', '6806-10-0', '6806-10-1', '6837-10-0', '6837-10-1', '695T-10-0', '695T-10-1', '695V-10-0', '695V-10-1', '709M-10-0', '709M-10-1', '709P-10-0', '7245-21-0', '736G-10-0', '736G-10-1', '736I-10-0', '736I-10-1', '746C-10-0', '746C-10-1', '746H-10-0', '746H-10-1', '746K-10-0', '746P-10-0', '746P-10-1', '746R-10-0', '8078-10-0', '807P-10-0', '875A-10-0', '875A-10-1', '875C-1-0', '875C-1-1', '875C-10-0', '875C-10-1', 'N505-11-0', 'N505-11-1', 'N506-11-0', 'N506-11-1', 'N604-11-0', 'N604-11-1', 'N701-11-0', 'N701-11-1', 'N702-11-0', 'N702-11-1', 'N704-11-0', 'N704-11-1', 'N705-11-0', 'N705-11-1', 'N731-11-0', 'N731-11-1', 'N742-11-0', 'N742-11-1']

#################################################################################################################
# agora, separar as trips que fazem uso dessas linhas e do metro
#"""
with open(TRIPS[1], 'r') as f:
    all_trips = xmltodict.parse(f.read())
metro_trips = [int(name['@name'].split('_')[1]) for name in all_trips['scsimulator_matrix']['multi_trip'] if name['@mode']=='metro']
trips_df_1 = pd.read_csv(TRIPS_DF[1], sep=';')
trips_usam_bus = []
trips_usam_bus_e_metro_certo = []
for trip_id in metro_trips:
    trip_info = trips_df_1.loc[trips_df_1['id_trip']==trip_id]
    # verificar se alguma das linhas de onibus faz parte dessa trip
    if any([l in list(trip_info['modo']) for l in linhas_interesse]):
        trips_usam_bus.append(trip_info)
        if any([e in list(trip_info['ini']) for e in estacoes_interesse_id]) or any([e in list(trip_info['fim']) for e in estacoes_interesse_id]):
            trips_usam_bus_e_metro_certo.append(trip_info)
##print(len(trips_usam_bus))  329
##print(len(trips_usam_bus_e_metro_certo))  136

nomes_trips_investigadas = []
for trip_data in trips_usam_bus_e_metro_certo:
    nomes_trips_investigadas.append(f'trip_{trip_data.iloc[0]["id_trip"]}')
#print(nomes_trips_investigadas)
#"""
# caso lilas com 400m  (136 trips)
#nomes_trips_investigadas = ['trip_1824', 'trip_1825', 'trip_2770', 'trip_3175', 'trip_6065', 'trip_7120', 'trip_7121', 'trip_7339', 'trip_7340', 'trip_7350', 'trip_7351', 'trip_7358', 'trip_7359', 'trip_7513', 'trip_7516', 'trip_8004', 'trip_8254', 'trip_8349', 'trip_8350', 'trip_8477', 'trip_8478', 'trip_8510', 'trip_8511', 'trip_8512', 'trip_8513', 'trip_8684', 'trip_8685', 'trip_11129', 'trip_11305', 'trip_12943', 'trip_12944', 'trip_13693', 'trip_13694', 'trip_14508', 'trip_14509', 'trip_15252', 'trip_16792', 'trip_17320', 'trip_17321', 'trip_17586', 'trip_17710', 'trip_17998', 'trip_17999', 'trip_18425', 'trip_18426', 'trip_18468', 'trip_18470', 'trip_18502', 'trip_18546', 'trip_18766', 'trip_18767', 'trip_18768', 'trip_18819', 'trip_18834', 'trip_18835', 'trip_19150', 'trip_19151', 'trip_19229', 'trip_19230', 'trip_19245', 'trip_19456', 'trip_19457', 'trip_19458', 'trip_19527', 'trip_19547', 'trip_19591', 'trip_20217', 'trip_20218', 'trip_20498', 'trip_21125', 'trip_21126', 'trip_21311', 'trip_21312', 'trip_21334', 'trip_21336', 'trip_21377', 'trip_21518', 'trip_21519', 'trip_21706', 'trip_21804', 'trip_21819', 'trip_21975', 'trip_22117', 'trip_22118', 'trip_22156', 'trip_22171', 'trip_22172', 'trip_22201', 'trip_22202', 'trip_22203', 'trip_22204', 'trip_22217', 'trip_22218', 'trip_22383', 'trip_22384', 'trip_22385', 'trip_22386', 'trip_22387', 'trip_22388', 'trip_22393', 'trip_22394', 'trip_22616', 'trip_22617', 'trip_22707', 'trip_22708', 'trip_22750', 'trip_22967', 'trip_22968', 'trip_23237', 'trip_23238', 'trip_23239', 'trip_23240', 'trip_23272', 'trip_23273', 'trip_23301', 'trip_23302', 'trip_23303', 'trip_23311', 'trip_23313', 'trip_23315', 'trip_23318', 'trip_23319', 'trip_23324', 'trip_23325', 'trip_23336', 'trip_23337', 'trip_23417', 'trip_23446', 'trip_23447', 'trip_23469', 'trip_23470', 'trip_23557', 'trip_23558', 'trip_23565', 'trip_23576', 'trip_24369']

#################################################################################################################
# calcular coisas pra essas trips investigadas (base x cenario)
events_0, events_1 = pl.scan_parquet(EVENTS[0]).with_row_index(), pl.scan_parquet(EVENTS[1]).with_row_index()
boarding_0, boarding_1 = pd.read_csv(BOARDING_TIMES[0]), pd.read_csv(BOARDING_TIMES[1])
tripsdf_0, tripsdf_1 = pd.read_csv(TRIPS_DF[0], sep=';'), pd.read_csv(TRIPS_DF[1], sep=';')

cols = ['nome_trip_base', f'nome_trip_{CENARIO_1}', 'mudou_estacao', 'espera_media0', 'espera_media1', 'tempo_total0', 'tempo_total1']
data = []
for trip in nomes_trips_investigadas:
    print(trip)
    # ver se sao diferentes (se as estacoes novas fizeram alguem andar menos)
    tdf1 = tripsdf_1.loc[tripsdf_1['id_trip']==int(trip.split('_')[1])]
    # como os ids podem mudar quando recalculo as trips, precisa ver a trip analoga certinho qual é
    n_id_trip = int(trip.split('_')[1])
    tdf0 = tripsdf_0.loc[tripsdf_0['id_trip']==n_id_trip]
    check = (tdf1.iloc[0]['horario'], tdf1.iloc[0]['fator'])
    i = 1
    while 1:  # pode dar erro dps, a trip pode nao existir no base
        deu_erro = False
        try:
            if (tdf0.iloc[0]['horario'], tdf0.iloc[0]['fator']) == check:   
                break
            tdf0_0 = tripsdf_0.loc[tripsdf_0['id_trip']==n_id_trip - i]
            if (tdf0_0.iloc[0]['horario'], tdf0_0.iloc[0]['fator']) == check: 
                tdf0 = tdf0_0
                break
            tdf0_1 = tripsdf_0.loc[tripsdf_0['id_trip']==n_id_trip + i]
            if (tdf0_1.iloc[0]['horario'], tdf0_1.iloc[0]['fator']) == check:
                tdf0 = tdf0_1
                break
        except:
            deu_erro = True
            break
        i += 1
    if deu_erro:
        continue
    
    metro_alterado = False
    if list(tdf0['ini']) != list(tdf1['ini']) or list(tdf0['fim']) != list(tdf1['fim']):
        metro_alterado = True
    
    # calcular tempo medio no ponto dessa trip
    nome_trip_0 = f"trip_{tdf0.iloc[0]['id_trip']}"
    nome_trip_1 = trip
    btrip0 = boarding_0.loc[boarding_0['agente'].fillna('').astype(str).str.startswith(str(nome_trip_0))]
    btrip1 = boarding_1.loc[boarding_1['agente'].fillna('').astype(str).str.startswith(str(nome_trip_1))]
    #btrip1 = boarding_1.loc[boarding_1['agente'].str.startswith(nome_trip_1)]
    mean_wait_time_0 = (btrip0['time_pegou_onibus'] - btrip0['time_chegou_ponto']).mean()
    mean_wait_time_1 = (btrip1['time_pegou_onibus'] - btrip1['time_chegou_ponto']).mean()
    
    if pd.isna(mean_wait_time_0) or pd.isna(mean_wait_time_1) or mean_wait_time_0 < 0 or mean_wait_time_1 < 0:
        continue
    
    # tempo total medio de viagem dessa trip
    arrivals0 = events_0.filter((pl.col('movetype')=='arrival')&(pl.col('agent').str.starts_with(nome_trip_0))).collect()
    arrivals1 = events_1.filter((pl.col('movetype')=='arrival')&(pl.col('agent').str.starts_with(nome_trip_1))).collect()
    mean_t_viagem_0 = arrivals0.select(pl.col('total_time').mean()).item()
    mean_t_viagem_1 = arrivals1.select(pl.col('total_time').mean()).item()
    
    data.append([nome_trip_0, nome_trip_1, metro_alterado, mean_wait_time_0, mean_wait_time_1, mean_t_viagem_0, mean_t_viagem_1])

pd.DataFrame(data, columns=cols).to_csv(SAIDA, sep=';', index=False)
