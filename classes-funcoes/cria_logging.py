import logging

def get_log(nome_log : str, caminho_destino : str):
    '''Cria Log no caminho e com o nome informado'''

    log = logging.getLogger(caminho_destino)
    log.setLevel(logging.info)

    if not log.handlers:
        handler = logging.FileHandler(caminho_destino)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        handler.setFormatter(formatter)
        log.addHandler(handler)

    return log