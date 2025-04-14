# main_runner.py

import subprocess

# Comandos para rodar os 3 scripts
scripts = [
    ["python3", "funding_rates.py"],
    ["python3", "cbbo_stream.py"],
    ["python3", "spread_calculator.py"]
]

processes = []

try:
    for cmd in scripts:
        proc = subprocess.Popen(cmd)
        processes.append(proc)

    # Espera todos os processos finalizarem (o que só acontece se forem parados manualmente)
    for proc in processes:
        proc.wait()

except KeyboardInterrupt:
    print("\n[INTERRUPT] Encerrando todos os scripts...")
    for proc in processes:
        proc.terminate()
