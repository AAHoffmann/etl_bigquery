from pathlib import Path
from cria_logging import get_log
import pandas as pd
import requests
import json


# Classe para gerenciar a etapa de Extract

LOG_DIR = Path(__file__).resolve().parent / "ETL/extract/logs"
DIR_FONTE = Path(__file__).resolve().parent / "ETL/data/fonte_dados"
DIR_FONTE_JSON = Path(__file__).resolve().parent / "ETL/data/fonte_dados/json"

_log_api_req = get_log("requisicao_api.log", LOG_DIR)
_log_api_to_csv = get_log("extract_api_and_save_to_csv.log", LOG_DIR)
_log_json_to_csv = get_log("extract_json_and_save_to_csv.log", LOG_DIR)


class Extract:
    """Classe para gerenciar a etapa de Extract via API"""

    def __init__(self, url_base_api: str, headers=None):
        self.url_base_api = url_base_api
        self.headers = headers if headers != None else None


    def _requisicao(self, endpoint_api, params=None):
        '''Realiza a requisição da API, mantem a lógica "encapsulada"'''
        url = f"{self.url_base_api}/{endpoint_api}"

        _log_api_req.info(f"Iniciando requisicao, URL ---> {url}")
        for tentativa in range(4):
            try:
                response_api = requests.get(url, headers=self.headers, params=params)

                if response_api.raise_for_status():
                    _log_api_req.error(f"{response_api.raise_for_status()} ---> {url}")

                _log_api_req.info(f"Requisicao efetuada com sucesso! URL ---> {url}")

                return response_api.json()
            except Exception as erro:
                _log_api_req.error(f"Ao realizar a requisicao, URL ---> {url}")
                _log_api_req.info(f"Tentativa numero {tentativa} para URL ---> {url}")
        _log_api_req.error(f"Tentativas excedidas para URL ---> {url}")
        raise Exception(f"Tentativas excedidas para URL ---> {url}")
        

    def extract_api(self, endpoint_api, params=None):
        """Retorna a requisição da API""" 
        return self._requisicao(endpoint_api, params)
    

    def _json_to_csv(self, dado_json, nome_csv):
        if isinstance(dado_json, list):             
            try:                
                df = pd.DataFrame(dado_json)
                df.to_csv(Path(DIR_FONTE) / f"{nome_csv}.csv", index=False)
            except Exception:
                _log_json_to_csv.error(f"Ao converter JSON para {nome_csv}.csv")
                pass            
        elif isinstance(dado_json, dict):
            try:
                df = pd.DataFrame(dado_json)
                df = [df]
                df.to_csv(Path(DIR_FONTE) / f"{nome_csv}.csv", index=False)
            except Exception:
                _log_json_to_csv.error(f"Ao converter JSON para {nome_csv}.csv")
                pass        


    def extract_api_and_save_to_csv(self, endpoint_api, nome_csv, params = None):
        """Retorna requisição da API e salva em CSV na pasta /data/fonte_dados"""
        dado_json = self._requisicao(endpoint_api, params)
        
        try:
            self._json_to_csv(dado_json, nome_csv)                            
        except Exception:
            _log_api_to_csv.error(f"Ao converter API para {nome_csv}.csv")
            pass  


    def extract_json_from_file_and_save_to_csv(self, nome):
        path = Path(DIR_FONTE_JSON) / f"{nome}.json", index=False
        
        if not Path(path).exists():
            _log_json_to_csv.error(f"Arquivo {nome}.json nao existe no caminho ---> {path}")
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
            _log_json_to_csv.error(f"Ao converter JSON para {nome}.csv")
            pass
 