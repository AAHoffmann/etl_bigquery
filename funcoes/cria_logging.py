from pathlib import Path
import logging
from datetime import datetime

def get_log(nome_log: str, caminho_destino: str):
    """Cria Log versionado por timestamp"""

    Path(caminho_destino).mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    nome_arquivo = f"{nome_log}_{timestamp}.log"
    caminho_arquivo = Path(caminho_destino) / nome_arquivo

    log = logging.getLogger(nome_log)
    log.setLevel(logging.INFO)

    if log.handlers:
        log.handlers.clear()

    handler = logging.FileHandler(caminho_arquivo)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    handler.setFormatter(formatter)
    log.addHandler(handler)

    return log