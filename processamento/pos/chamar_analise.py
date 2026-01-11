
import sys
import time
import subprocess as sp

CENARIO_0 = sys.argv[1]
CENARIO_1 = sys.argv[2]
COR = sys.argv[3]
# trips_alteradas
"""
scripts = [['python3', f'trips_alteradas.py', CENARIO_0, CENARIO_1,
            str(i), COR] for i in range(10)]
processos = [sp.Popen(script) for script in scripts]
for p in processos:
    p.wait()
"""
# pontos_proximos
scripts = [['python3', f'pontos_proximos.py', CENARIO_0, CENARIO_1,
            str(i), COR] for i in range(10)]
processos = [sp.Popen(script) for script in scripts]
for p in processos:
    p.wait()