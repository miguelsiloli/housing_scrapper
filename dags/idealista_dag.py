from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from ..idealista.scraper_base import scraper_factory 
from ..idealista.parser import main_function, parse_html_files_to_dataframe
import boto3
import shutil
import os
import pandas as pd


idealista_urls = pd.read_csv("district_data_updated.csv")["neighborhood_link"]

def scrape_idealista(urls, directory_path):
    idealista_scraper = scraper_factory('idealista')
    idealista_scraper.scrape(urls= urls, 
                             directory_path= directory_path)
    
def delete_folder(folder_path):
    """
    Deletes the specified folder and all its contents.

    Parameters:
    folder_path (str): The path to the folder to be deleted.
    """
    # Check if the folder exists
    if os.path.exists(folder_path):
        # Remove the folder and all its contents
        shutil.rmtree(folder_path)
        print(f"Folder '{folder_path}' has been deleted.")
    else:
        print(f"Folder '{folder_path}' does not exist.")

s3 = boto3.client('s3',
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        region_name=os.getenv('region_name'))

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

parse_task = PythonOperator(
    task_id='parse_idealista',
    python_callable=main_function,
    op_kwargs={'parsing_function': parse_html_files_to_dataframe, 
               's3_object': s3,
               'source_directory_path': "./raw/idealista"},
    dag=dag
)

delete_task = PythonOperator(
    task_id='delete_idealista',
    python_callable= delete_folder,
    op_kwargs={'folder_path': "./raw/idealista"},
    dag=dag
)

scrape_task >> parse_task >> delete_task