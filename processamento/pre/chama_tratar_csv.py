import sys
import subprocess as sp
import time

#time.sleep(2*3600)


CENARIO = sys.argv[1]

# arrumar
#scripts = [['python3', f'arrumar_events.py', CENARIO, str(i)] for i in range(10)]
#processos = [sp.Popen(script) for script in scripts]
#for p in processos:
#    p.wait()
# onibus_pegados
scripts = [['python3', f'obter_onibus_pegados.py', CENARIO, str(i)] for i in range(10)]
processos = [sp.Popen(script) for script in scripts]
for p in processos:
    p.wait()
# timelines
scripts = [['python3', f'obter_timelines.py', CENARIO, str(i)] for i in range(10)]
processos = [sp.Popen(script) for script in scripts]
for p in processos:
    p.wait()
    
    
sp.run(["python3", "chamar_analise.py" ,'base', CENARIO, 'azul'], cwd="/home/cassianodragao/scripts/analise")