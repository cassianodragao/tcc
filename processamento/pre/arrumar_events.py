
import sys
import pandas as pd
import polars as pl

CENARIO = sys.argv[1]
NUMERO  = sys.argv[2]

EVENTS         = f'/home/cassianodragao/scripts/dados/{CENARIO}/events/events_{CENARIO}_{NUMERO}.xml'                                                                                    #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/saida/events_{CENARIO}.xml'
EVENTS_CSV     = f'/home/cassianodragao/scripts/dados/{CENARIO}/events_csv/events_{CENARIO}_{NUMERO}.csv'                                                                             #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/saida/events_{CENARIO}.csv'
EVENTS_PARQUET = f'/home/cassianodragao/scripts/dados/{CENARIO}/events_parquet/events_{CENARIO}_{NUMERO}.parquet'                                                                           #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/saida/events_{CENARIO}.parquet'
TEMP           = f'/home/cassianodragao/scripts/dados/{CENARIO}/events_arrumado/events_{CENARIO}_{NUMERO}.tmp'                                                                                      #f'D:/TCC_NOVO/CRIANDO_CENARIO/cenarios/{CENARIO}/saida/temp.xml'

##############################################################################################################

# remover o ';'
input_path = EVENTS
output_path = TEMP
with open(input_path, "r", encoding="utf-8") as fin, open(output_path, "w", encoding="utf-8") as fout:
    l = 0
    for line in fin:
        #print(l)
        fout.write(line.replace(":", ";"))
        l += 1
##############################################################################################################
# arrumar as linhas que tem menos colunas que as outras
infile = TEMP
outfile = EVENTS_CSV
with open(infile, 'r') as fin, open(outfile, 'w') as fout:
    fout.write('time;movetype;agent;link;total_time;total_walked\n')
    for line in fin:
        line = line[:-1] # retirar quebra de linha
        splited = line.split(';')
        if len(splited) == 5: #move metro
            newline = splited[1: ] + ['0', '0\n']
            movetime = int(newline[0])
            i = 1
            while movetime > time_atual + 200:
                movetime = int(newline[0][i: ])
                i += 1
            newline[0] = str(movetime)
        elif len(splited) == 6: # move trip
            newline = splited[2: ] + ['0', '0\n']
        elif len(splited) == 8:
            newline = splited[2: -1] + [splited[-1]+'\n']
        else:
            raise Exception('aisdoiasd')
            
        
        time_atual = int(newline[0])
        
        
        nl = newline
        lineout = f'{nl[0]};{nl[1]};{nl[2]};{nl[3]};{nl[4]};{nl[5]}'
        fout.write(lineout)
        
##############################################################################################################
# transformar em parquet pra ler depois
csv = EVENTS_CSV
parquet = EVENTS_PARQUET
pl.read_csv(csv, separator=";").write_parquet(parquet)