from pathlib import Path
from funcoes.cria_logging import get_log 
import pandas as pd
import requests
import json


# Classe para gerenciar a etapa de Extract
LOG_DIR = Path(__file__).resolve().parent / "logs"
DIR_FONTE_API = Path(__file__).resolve().parents[2] / "data/fonte_dados/dados_api"
DIR_FONTE_JSON = Path(__file__).resolve().parents[2] / "data/fonte_dados/json"


class Extract:
    """Classe para gerenciar a etapa de Extract"""    
    def __init__(self):
            # Os logs só serão "ativados" quando você fizer: extract = Extract()
            self._log_api_req = get_log("requisicao_api.log", LOG_DIR)
            self._log_api_to_csv = get_log("extract_api_and_save_to_csv.log", LOG_DIR)
            self._log_json_to_csv = get_log("json_to_csv.log", LOG_DIR)

    # def __init__(self, url_base_api=None, headers=None):
    #     self.url_base_api = url_base_api
    #     self.headers = headers if headers != None else None


    def _requisicao(self, url, endpoint_api=None, params=None, headers=None):
        '''Realiza a requisição da API, mantem a lógica "encapsulada"'''
        if endpoint_api is not None:
            url = f"{url}/{endpoint_api}"
        else:
            url = url

            self._log_api_req.info(f"Iniciando requisicao, URL ---> {url}")
        for tentativa in range(4):
            try:
                response_api = requests.get(url, headers=headers, params=params)

                if response_api.raise_for_status():
                    self._log_api_req.error(f"{response_api.raise_for_status()} ---> {url}")

                self._log_api_req.info(f"Requisicao efetuada com sucesso! URL ---> {url}")

                return response_api.json()
            except Exception as erro:
                 self._log_api_req.error(f"Ao realizar a requisicao, URL ---> {url}")
                 self._log_api_req.info(f"Tentativa numero {tentativa} para URL ---> {url}")
            self._log_api_req.error(f"Tentativas excedidas para URL ---> {url}")
        raise Exception(f"Tentativas excedidas para URL ---> {url}")
        

    def extract_api(self, url, endpoint_api=None, params=None, headers=None):
        """Retorna a requisição da API""" 
        return self._requisicao(url, endpoint_api, params, headers)
    

    def _json_to_csv(self, dado_json, nome_csv):
        if isinstance(dado_json, list):             
            try:                
                df = pd.DataFrame(dado_json)
                caminho = Path((DIR_FONTE_API) / f"{nome_csv}.csv", index=False)
                df.to_csv(caminho)
            except Exception:
                self._log_json_to_csv.error(f"Ao converter JSON para {nome_csv}.csv --> {caminho}",exc_info=True)
                pass            
        elif isinstance(dado_json, dict):
            try:
                df = pd.DataFrame(dado_json)
                df = [df]
                df.to_csv(Path(DIR_FONTE_API) / f"{nome_csv}.csv", index=False)
            except Exception:
                self._log_json_to_csv.error(f"Ao converter JSON para {nome_csv}.csv")
                pass        


    def extract_api_and_save_to_csv(self, url, endpoint_api, nome_csv, params = None, headers = None):
        """Retorna requisição da API e salva em CSV na pasta /data/fonte_dados"""        
        dado_json = self._requisicao(url, endpoint_api, params, headers)
        self._log_api_to_csv.info(f"Iniciando conversao da {url} ---> {nome_csv}.csv")
        
        try:
            self._json_to_csv(dado_json, nome_csv)                            
        except Exception:
            self._log_api_to_csv.error(f"Ao converter API para {nome_csv}.csv")
            pass  
        self._log_api_to_csv.info(f"Concluida conversao da {url} ---> {nome_csv}.csv")

    def extract_json_from_file_and_save_to_csv(self, nome):
        path = Path(DIR_FONTE_JSON) / f"{nome}.json"
        
        if not Path(path).exists():
            self._log_json_to_csv.error(f"Arquivo {nome}.json nao existe no caminho ---> {path}")
            raise FileNotFoundError(f"Arquivo {nome}.json nao existe no caminho ---> {path}")            
        else:
            with open(path, "r", encoding="utf-8") as arquivo:
                try:
                    dado_json = json.load(arquivo)
                except json.JSONDecodeError:
                    raise ValueError(f"Arquivo {nome}.json invalido, no caminho ---> {path}")

        try:
            self._json_to_csv(dado_json, nome)
        except Exception:
            self._log_json_to_csv.error(f"Ao converter JSON para {nome}.csv")
            pass
 