from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import json



# Loop to prepare actor name for API request
def request_actor_id():

    actor = 'George Clooney'	
    actor_name = ''
    for i in actor:
      if i ==' ':
        i='%20'
        actor_name += i
      else:
        actor_name += i
    #insert your API key in the link below		
    actor_request_url = "https://api.themoviedb.org/3/search/person?api_key=[insert API key here]&language=en-US&query=" + str(actor_name) + "&include_adult=false"

    response = requests.request("GET", actor_request_url)

    # reading the response and pulling out the list in the dictionary
    response = response.json()
    response = response['results']
    response = response[0]

    # pulling out the actor id
    actor_id = response['id']
    to_text_file = "{name},{id}".format(name = actor_name, id = actor_id)
 
    write_id_to_file(to_text_file)

def write_id_to_file(actor_id):
    f = open("actor_id.txt", "w")
    f.write(actor_id)
    f.close()

def read_actor_id_file():
    f = open("actor_id.txt", "r")
    to_text_file= f.read()
    return to_text_file

def write(jsonfile, data):
    with open(jsonfile, 'w') as outfile:
        json.dump(data, outfile)

def request_movies():
    to_text_file = read_actor_id_file()
    list_from_file = to_text_file.split(",")
    actor_name = list_from_file[0]
    actor_id = list_from_file[1]
    #insert your API key in the link below
    request_url = "https://api.themoviedb.org/3/person/" + str(actor_id) + "/movie_credits?api_key=[insert your api key here]&language=en-US"

    movies = requests.request('GET',request_url)
    movies = movies.json()

    json_name = "/Users/deana/final project/Airflow/" + str(actor_name) +".json"
    write(json_name,movies)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['deana.stuart16@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),}

dag = DAG(
    'my_improved_dag',
    default_args=default_args,
    description='my movie dag',
    schedule_interval=timedelta(days=1),
)
with DAG('my_improved_dag',
         default_args=default_args,
         schedule_interval='*/10 * * * *',
         ) as dag:

    actor_id_request = PythonOperator(task_id='actor_id_request',
                               python_callable=request_actor_id)
    movie_list_request =PythonOperator(task_id='request_movie_list',
                               python_callable=request_movies)

actor_id_request>>movie_list_request


