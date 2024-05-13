from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scraper_base import scraper_factory  # Assume this is where scraper_factory is define

idealista_urls = [
    "https://www.idealista.pt/arrendar-casas/aveiro/",
    "https://www.idealista.pt/arrendar-casas/coimbra/",
    "https://www.idealista.pt/arrendar-casas/viseu/",
    "https://www.idealista.pt/arrendar-casas/viana-do-castelo/",
    "https://www.idealista.pt/arrendar-casas/maia/", 
    "https://www.idealista.pt/arrendar-casas/cascais/",
    "https://www.idealista.pt/arrendar-casas/sintra/",
    "https://www.idealista.pt/arrendar-casas/leiria/",
    "https://www.idealista.pt/arrendar-casas/matosinhos/",
    "https://www.idealista.pt/arrendar-casas/vila-nova-de-gaia/",
    "https://www.idealista.pt/arrendar-casas/loures/",
    "https://www.idealista.pt/arrendar-casas/almada/",
    "https://www.idealista.pt/arrendar-casas/setubal/",
    "https://www.idealista.pt/arrendar-casas/guimaraes/",
    "https://www.idealista.pt/arrendar-casas/gondomar/"
]

def scrape_idealista(urls, directory_path):
    idealista_scraper = scraper_factory('idealista')
    idealista_scraper.scrape(urls= urls, 
                             directory_path= directory_path)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['miguel99silva@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'idealista_scraping_dag',
    default_args=default_args,
    description='Daily Idealista Scraping at 12 PM',
    schedule_interval='0 12 * * *',  # At 12:00 PM every day
    catchup=False
)

# Define the task
scrape_task = PythonOperator(
    task_id='scrape_idealista',
    python_callable=scrape_idealista,
    op_kwargs={'urls': idealista_urls, 
               'directory_path': 'raw/idealista'},
    dag=dag
)