from pathlib import Path
import pandas as pd
import logging

#Constantes
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "fonte_dados"
OUTPUT_DIR = BASE_DIR / "dados_tratados"
OUTPUT_DIR.mkdir(exist_ok=True)
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

#Variáveis
linhas_totais = 0
qtd_csv = 0

#Funções
def processar_arquivo(caminho):
    """
    Lê arquivo CSV. Realiza alguns tratamentos nos registros.
    """
    df = pd.read_csv(caminho)
    df.columns = [coluna.lower().strip() for coluna in df.columns]
    df = df.drop_duplicates()

    #Verificar se alguma coluna do csv possui date ou timestamp no nome, assim transforma para o tipo datetime
    for coluna in df.columns:
        if any(palavra in coluna for palavra in ["date", "timestamp"]):
            df[coluna] = pd.to_datetime(df[coluna], errors="coerce")

            #Caso o any acima retornar True e entrar na condição, cria coluna de mês e ano
            df["mes"] = df[coluna].dt.month
            df["ano"] = df[coluna].dt.year

    return df

#Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "processa_csv.log"),
        logging.StreamHandler()
    ]
)


#Processamento do ETL
logging.info("Iniciando Processamento")

for arquivo in DATA_DIR.rglob("*.csv"):
    try:
        logging.info(f"Processando arquivo -> {arquivo.name}")
        
        df = processar_arquivo(arquivo)    
        arquivo_saida = OUTPUT_DIR / arquivo.name
        df.to_csv(arquivo_saida, index=False)
        
        linhas_totais += len(df)

        logging.info(f"Processado {arquivo.name} Linhas: {len(df)} -> Salvo em {arquivo_saida}")        

        qtd_csv += 1
    except Exception as erro:
        logging.error(f"Erro durante o processamento do arquivo {arquivo.name}: {erro}")

logging.info(f"Total de registros: {linhas_totais}")
logging.info(f"Quantidade de arquivos CSV processados: {qtd_csv}")
logging.info("Processo de ETL finalizado")


"""
Processa todos os CSV da pasta fonte_dados, faz um breve tratamento nos dados e salva na pasta dados_tratados.

Os tratamentos estão generalizados nessa etapa, o objetivo é realizar um breve tratamento para em seguida ter os dados disponibilizados em um Data Lake.

Uma vez no Data Lake, é criada as tabelas dimensões e então a tabela fato. A partir dessas tabelas, é montado scripts SQL conforme necessidade.
"""

