import os
import json

def get_config(file_name):
    # Caminho completo para o arquivo de configuração
    config_path = os.path.join(os.path.dirname(__file__),  '../../config', f'{file_name}.json')
    try:
        with open(config_path, 'r') as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"O arquivo {config_path} não foi encontrado.")
        return None
    except json.JSONDecodeError:
        print(f"Erro ao decodificar o JSON no arquivo {config_path}.")
        return None

