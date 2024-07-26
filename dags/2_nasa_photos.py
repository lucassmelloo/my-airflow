from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from helpers.helper import get_config

from mysql.connector import Error
import pprint
import mysql.connector
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
        bash_command = 'mkdir -p ./shared/nasa_photos/photos'
    )

    test_db_connection = MySqlOperator(
        task_id='test_db_connection',
        mysql_conn_id= 'mysql_docker_localhost',
        sql='SELECT 1;'
    )

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
                            image LONGBLOB,
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
    
    @task
    def extract_api_data():
        nasa_api_key = Variable.get('NASA_API_KEY')
        URL = f'https://api.nasa.gov/mars-photos/api/v1/rovers/curiosity/latest_photos?api_key={nasa_api_key}'
        print(URL)
        response = requests.get(URL)
        print(response)
        if  response.status_code == 200:
            data = response.json()
            latest_photos = data.get('latest_photos', [])
            return latest_photos
        
    @task
    def save_image_on_folder(image_array):
        folder_path = f'./shared/nasa_photos/photos'
        os.makedirs(folder_path, exist_ok=True)
        
        for image in image_array:
            image_url = image['img_src']
            image_id = image['id']
            image_path = os.path.join(folder_path, f'{image_id}.jpg')
            image['img_path'] = os.path.join(folder_path, f'{image_id}.jpg')
            
            response = requests.get(image_url)
            if response.status_code == 200:
                with open(image_path, 'wb') as file:
                    file.write(response.content)
            else:
                print(f"Failed to download image {image_id}")

        return image_array

    @task
    def save_image_on_database(image_array):
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
            cursor = connection.cursor()
            for image in image_array:

                cursor.execute(
                    '''
                        SELECT COUNT(*)
                        FROM nasa_photos
                        WHERE id_photo = %s
                    ''',( image['id'],)
                )

                result = cursor.fetchone()
                
                if result[0]==0 :

                    with open(image['img_path'], 'rb') as file:
                        photo_content = file.read()
                    
                    cursor.execute(
                        '''
                            INSERT INTO nasa_photos (id_photo, image, json, created_at)
                            VALUES (%s,%s,%s,%s)
                        ''',
                        (image["id"], photo_content ,json.dumps(image), pendulum.today().to_datetime_string())
                    )

                    connection.commit()

                else:
                    continue

        except Error as e:
            print(f"Erro inserir registros no MySQL: {e}")

    
    extracted_data = extract_api_data()
    preare_database = prepare_database_for_images()
    saving_images_on_folder = save_image_on_folder(extracted_data)
    saving_images_on_database = save_image_on_database(saving_images_on_folder)

    create_images_folder >> preare_database
    test_db_connection >> preare_database
    preare_database >> extracted_data >> saving_images_on_folder >> saving_images_on_database
