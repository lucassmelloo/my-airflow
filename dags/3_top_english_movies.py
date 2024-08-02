import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
import os
from datetime import datetime, timedelta
import sys
from helpers.scrape_imdb_charts import _get_soup, _scrape_movies ,_load_to_bigQuery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/BigQuery/key/astute-asset-419023-604bcc14bdd5.json'

default_args = {
    'start_date': datetime.today(),
    'schedule_interval': '0 0 * * *', #Run everyday at midnight
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG (dag_id='3_top_english_movies', default_args=default_args, catchup=False) as dag:

    @task
    def scrape_movies():
        soup = _get_soup('top_english_movies')
        movies_df = _scrape_movies(soup=soup)
        return movies_df
    
    @task
    def load_movies(movies_df):
        _load_to_bigQuery(movies_df, 'top_english_movies')

    movies_df = scrape_movies()
    load_movies(movies_df=movies_df)