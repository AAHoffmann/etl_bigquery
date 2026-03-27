from pandas_gbq import to_gbq
from pathlib import Path
from google.oauth2 import service_account
import pandas as pd
import logging
import time

class ErroETL(Exception):
    pass

#Cria conxeção atráves de arquivo JSON com o BigQuery
credenciais_acesso = service_account.Credentials.from_service_account_file(
    Path(__file__).resolve().parent/"credenciais.json"
)

#Constantes
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "dados_tratados"
LOG_DIR = BASE_DIR / "logs"


LOG_NAME = LOG_DIR / "carga_data_cloud.log"


#Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "carga_data_cloud.log"),
        logging.StreamHandler()
    ]
)

#Funções
def transf_to_bigquery(df : pd.DataFrame,  tabela_destino : str, id_projeto : str, replace_tabela : bool):

    to_gbq(
        df,
        destination_table=f"etl_data_set.{tabela_destino}",
        project_id=id_projeto,
        if_exists= "replace" if replace_tabela else "fail"
    ) 

def retry(funcao, tentativas=4, delay_base=1, delay_expoente=2):
    for tentativa in range(1, tentativas + 1):
        try:
            return funcao()
        
        except Exception as erro:
            logging.info("Falha ao Transferir para o BigQuery")
            logging.info(f"[Tentativa número {tentativa}] -> Erro: {erro}")
            
            if tentativa == tentativas:                
                raise ErroETL("Tentativas maximas de transferecia atingidas")
                
            
            tempo_espera = delay_base * (delay_expoente ** (tentativa - 1))
            logging.info(f"Aguardando {tempo_espera}s para tentar novamente!")
            time.sleep(tempo_espera)


#Inicio do processamento
logging.info("Iniciando Transferencia para o BigQuery!")
for arquivo in DATA_DIR.rglob("*.csv"):
    try:
        df = pd.read_csv(arquivo)

        nome_tabela = arquivo.name.replace("olist_","").replace("_dataset.csv", "").strip().lower()

        #Enviando dataset para o BigQuery
        logging.info(f"Transferindo -> CSV: {arquivo.name} para tabela: {nome_tabela} no BigQuery")
        transf_to_bigquery(df, nome_tabela, "etlolist", True)
        
        logging.info(f"Concluida transferencia -> CSV: {arquivo.name} para tabela: {nome_tabela}")

    except Exception as erro:
        logging.error(f"Erro de transferencia do CSV {arquivo.name} para tabela {nome_tabela} no BigQuery, nao foi possivel transferir")

        



   