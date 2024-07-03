import airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
from helper.scrape_imdb_charts import _get_soup, _scrape_movies, _load_to_bigQuery

# Creating an Environmental variable for the service key configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/BigQuery/key/astute-asset-419023-604bcc14bdd5.json'

default_args = {
    'start_date': datetime.datetime.today(),
    'schedule_interval': '0 0 * * *', #Run everyday at midnight
    'retries': 5,
    'retry_delay': timedelta(minutes=5) 
}

with DAG (dag_id= '3_top_250_movies', default_args=default_args, catchup= False) as dag:
    
    # Dag #1: Get the monst popular movies
    @task
    def scrape_movies():
        soup = _get_soup(chart='top_250_movies')
        movie_df = _scrape_movies(soup)
        return movie_df
    
    #Dag #2: Load the most popular movies
    @task
    def load_movies(movies_df):
        _load_to_bigQuery(movies_df, chart='top_250_movies')

    # Dependencies
    movies_df = scrape_movies()
    load_movies(movies_df)