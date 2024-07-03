from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.macros import ds_add

import os
import json
import pendulum
import requests


with DAG(
    "2_fotos_nasa",
    start_date=pendulum.datetime(2024, 5, 6, tz="UTC"),
    schedule_interval='0 0 * * 1'
) as dag:
    
    task_1 = BashOperator(
        task_id='create_dag_folder',
        bash_command = 'mkdir -p ./shared/nasa_photos/week={{data_interval_end}}'
    )

    def get_key():
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'nasa_key.json')
        try:
            with open(config_path, 'r') as file:
                data = json.load(file)
                return data.get('api_key')
        except FileNotFoundError:
            print(f"O arquivo {config_path} não foi encontrado.")
            return None
        except json.JSONDecodeError:
            print(f"Erro ao decodificar o JSON no arquivo {config_path}.")
            return None

    def extract_data(data_interval_end):
        api_key = get_key()

        URL = f'https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/latest_photos?api_key={api_key}'
        
        response = requests.get(URL)
        #print(response[1].id)
        if  response.status_code == 200:
            data = response.json()
            latest_photos = data.get('latest_photos', [])
            img_srcs = [photo['img_src'] for photo in latest_photos if 'img_src' in photo]
        
        first_img = requests.get(img_srcs[1])

        return first_img
        

    task_2 = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={
            'data_interval_end': ''
        }
    )

    task_1 >> task_2