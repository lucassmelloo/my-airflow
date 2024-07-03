import requests
from bs4 import BeautifulSoup
import os
import sys
import re
from google.cloud import bigquery
import datetime
import pandas as pd

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/config/bigquery.json'

bigquery_client = bigquery.Client()
def _get_soup(chart):

    if chart == 'most_popular_movies':
        url = 'https://www.imdb.com/chart/moviemeter?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=5V6VAGPEK222QB9E0SZ8&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=toptv&ref_=chttvtp_ql_2'
    
    if chart == 'top_250_movies' :
        url ='https://www.imdb.com/chart/top?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=5V6VAGPEK222QB9E0SZ8&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=toptv&ref_=chttvtp_ql_3'
    
    if chart == 'top_english_movies' :
        url = 'https://www.imdb.com/chart/top-english-movies?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=3YMHR1ECWH2NNG5TPH1C&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=boxoffice&ref_=chtbo_ql_4'
    
    if chart == 'top_250_tv' :
        url = 'https://www.imdb.com/chart/tvmeter?pf_rd_m=A2FGELUUNOQJNL&pf_rd_p=470df400-70d9-4f35-bb05-8646a1195842&pf_rd_r=J9H259QR55SJJ93K51B2&pf_rd_s=right-4&pf_rd_t=15506&pf_rd_i=topenglish&ref_=chttentp_ql_5'


    response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
    soup = BeautifulSoup(response.text, 'html.parser')
    return soup


def _scrape_movies(soup) :

    movies_names = []
    movies_years = []
    movies_ratings = []
    user_votings = []

    filmList = soup.findAll('li', {'class':'ipc-metadata-list-summary-item sc-10233bc-0 iherUv cli-parent'})


    for film in filmList :
    
        #Getting film title
        try:
            title = film.find('h3', {'class': 'ipc-title__text'}).text.split('. ', 1)[-1]
            movies_names.append(title)
        except:
            print('Missing Title. Replacing with -1')
            movies_names.append(-1)

        #Getting film year
        try:
            year = film.find('span', {'class': 'sc-b189961a-8 kLaxqf cli-title-metadata-item'}).text[:4]
            movies_years.append(int(year))
        except:
            print("Missing Year. Replacing with -1")
            movies_years.append(-1)

        #Getting ratting
        try:
            ratting = film.find('span', {'class': 'ipc-rating-star ipc-rating-star--base ipc-rating-star--imdb ratingGroup--imdb-rating'}).text[:3]
            movies_ratings.append(float(ratting))
        except:
            print('Missing rating. Raplcing with -1')
            movies_ratings.append(-1)

        #Gettting votes
        try:
            votes_str= film.find('span', {'class': 'ipc-rating-star--voteCount'}).text
            votes_str= re.sub(r"\(|\)", "", votes_str)
            votes_int= int(_unit_converter(votes_str))
            user_votings.append(votes_int)
        except:
            user_votings.append(-1)

    movie_df = pd.DataFrame({
        'movie_name': movies_names,
        'movie_year': movies_years, 
        'movie_rating': movies_ratings,
        'user_votings': user_votings
        })
    
    movie_df['movie_id']= movie_df.index + 1

    movie_df['update_date']= datetime.datetime.today().strftime('%Y-%m-%d')

    movie_df = movie_df[['movie_id','movie_name','movie_year','movie_rating','user_votings', 'update_date']]
    print(movie_df)
    return movie_df

def _unit_converter(string):
    if string[-1] == 'M':
        return int(float(string[:-1])* 1000000)
    if string[-1] == 'K':
        return int(float(string[:-1])* 1000)
    else:
        return int(float(string[:-1]))



def _getOrCreate_dataset(dataset_name :str) -> bigquery.dataset.Dataset:
     print('Fetching Dataset...')

     try:
        dataset = bigquery_client.get_dataset(dataset_name)
        print('Done')
        print(dataset.self_link)
        return dataset
     
     except Exception as e:
         if e.code == 404:
             print('Dataset does not exist. Creating a new one.')
             bigquery_client.create_dataset(dataset_name)
             dataset =  bigquery_client.get_dataset(dataset_name)
             print('Done.')
             print(dataset.self_link)
             return dataset
         else:
             print(e)

def _getOrCreate_table(dataset_name:str, table_name:str) :
    dataset= _getOrCreate_dataset(dataset_name)
    project = dataset.project
    dataset = dataset.dataset_id
    table_id = project + '.' + dataset + '.' + table_name

    print('Fetching Table...')

    try:
        table = bigquery_client.get_table(table_id)
        print('Done')
        print(table.self_link)

    except Exception as e:
        
        if e.code == 404:
            print('Table does not exists. Creating a new one.')
            bigquery_client.create_table(table_id)
            table= bigquery_client.get_table(table_id)
            print(table.self_link)

        else:
            print(e)

    finally:
        return table
    
def _load_to_bigQuery(movie_names, chart, dataset_name='imdb'):

    if chart == 'most_popular_movies':
        table_name = 'most_popular_movies'
    
    if chart == 'top_250_movies':
        table_name = 'top_250_movies'

    if chart == 'top_english_movies':
        table_name = 'top_english_movies'

    if chart == 'top_250_tv':
        table_name = 'top_250_tv'
    
    table = _getOrCreate_table(dataset_name, table_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=[
            bigquery.SchemaField("movie_id", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("movie_name", bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("movie_year", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("movie_rating", bigquery.enums.SqlTypeNames.FLOAT64),
            bigquery.SchemaField("user_votings", bigquery.enums.SqlTypeNames.INT64),
            bigquery.SchemaField("update_date", bigquery.enums.SqlTypeNames.DATETIME),
        ],
        write_disposition="WRITE_TRUNCATE"
    )
    job = bigquery_client.load_table_from_dataframe(
        movie_names,table,job_config=job_config
    )

    job.result()

    print("Loaded {} rows into {}:{}".format(job.output_rows,dataset_name,table_name))
    

#most_popular_movies
#top_250_movies
#top_english_movies
#top_250_tv
soup = _get_soup(chart='top_250_tv')
movies_df = _scrape_movies(soup)
dataset = _load_to_bigQuery(movies_df,chart= 'top_250_tv')