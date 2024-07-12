from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.providers.mysql.operators.mysql import MySqlOperator
from helpers.helper import get_config

import mysql.connector
from mysql.connector import Error
import os
import json
import pendulum
import requests


with DAG(
    "2_nasa_photos",
    start_date=pendulum.datetime(2024, 5, 6, tz="UTC"),
    schedule_interval='0 0 * * 1'
) as dag:
    
    create_images_folder = BashOperator(
        task_id='create_dag_folder',
        bash_command = 'mkdir -p ./shared/nasa_photos/week={{data_interval_end}}'
    )

    test_db_connection = MySqlOperator(
        task_id='test_db_connection',
        mysql_conn_id= 'mysql_docker_localhost',
        sql='SELECT 1;'
    )
    
    @task
    def extract_api_data():
        nasa_config = get_config('nasa_key')
        URL = f'https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/latest_photos?api_key={nasa_config['api_key']}'
        
        response = requests.get(URL)
        if  response.status_code == 200:
            data = response.json()
            latest_photos = data.get('latest_photos', [])
            return latest_photos
        
    @task
    def save_image_on_folder(image_array):
        folder_path = f'./shared/nasa_photos/week={{data_interval_end}}'
        os.makedirs(folder_path, exist_ok=True)
        
        for image in image_array:
            image_url = image['img_src']
            image_id = image['id']
            image_path = os.path.join(folder_path, f'{image_id}.jpg')
            
            response = requests.get(image_url)
            if response.status_code == 200:
                with open(image_path, 'wb') as file:
                    file.write(response.content)
            else:
                print(f"Failed to download image {image_id}")

    @task
    def prepare_database_for_images():
        mysql_infos = get_config('mysql_nasa_connection')

        try:
            connection = mysql.connector.connect(
                host=mysql_infos["host"],
                port=mysql_infos["port"],
                user=mysql_infos["user"],
                password=mysql_infos["password"],
                database=mysql_infos["schema"]
            )
            
        except Error as e:
             print(f"Erro ao conectar ao MySQL: {e}")
        
        try:

            print(f"Verificando existencia da tabela...")
            cursor = connection.cursor()
            cursor.execute(
                '''
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = %s
                    AND table_name = %s;
                ''', 
                (mysql_infos["schema"],'nasa_photos')
            )
            
            result = cursor.fetchone()
            
            if result[0]==0:

                print(f"Criando Tabela...")

                cursor.execute(
                    '''
                        CREATE TABLE nasa_photos 
                        (
                            id BIGINT AUTO_INCREMENT PRIMARY KEY,
                            id_photo INT UNIQUE,
                            image BLOB,
                            json TEXT,
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                            UNIQUE(id_photo)
                        );
                    ''')
            
                print(f"Tabela criada...")

        except Error as e:

            print(f"Erro ao conectar ao MySQL: {e}")
            
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("Conexão ao MySQL encerrada")
    
    extracted_data = extract_api_data()
    save_images = save_image_on_folder(extracted_data)
    preare_database = prepare_database_for_images()

    create_images_folder >> preare_database
    test_db_connection >> preare_database
    preare_database >> extracted_data >> save_images
