from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
from ..imovirtual.graphql_main import fetch_imovirtual_data, get_buildid
from ..imovirtual.parse_data import process_and_upload_data_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'imovirtual_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline for fetching and processing imovirtual data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

fetch_data = PythonOperator(
    task_id='fetch_imovirtual_data',
    python_callable=fetch_imovirtual_data,
    op_kwargs={
        'csv_file_path': 'imovirtual_catalog.csv',
        'output_dir': 'raw/imovirtual/',
        'headers': {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
            "Accept": "multipart/mixed, application/graphql-response+json, application/graphql+json, application/json",
            "Accept-Language": "pt-PT,pt;q=0.8,en;q=0.5,en-US;q=0.3",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Content-Type": "application/json",
            "baggage": "sentry-environment=imovirtualpt2-prd,sentry-release=frontend-platform%40v20240603T121501-imovirtualpt2,sentry-public_key=feffe528c390ea66992a4a05131c3c68,sentry-trace_id=cc8c6f78b99b4e959c670ca3a2b379bd,sentry-transaction=%2Fpt%2Fresultados%2F%5B%5B...searchingCriteria%5D%5D,sentry-sampled=false",
            "Origin": "https://www.imovirtual.com",
            "Alt-Used": "www.imovirtual.com",
            "Connection": "keep-alive",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "Priority": "u=1",
            "TE": "trailers"
        },
        'base_url_template': "https://www.imovirtual.com/_next/data/{}/pt/resultados/arrendar/apartamento/{}.json",
        'get_buildid': get_buildid
    },
    dag=dag,
)

process_and_upload = PythonOperator(
    task_id='process_and_upload_data_to_s3',
    python_callable=process_and_upload_data_to_s3,
    op_kwargs={
        'source_path': "raw/imovirtual/",
        'aws_access_key_id': os.getenv('aws_access_key_id'),
        'aws_secret_access_key': os.getenv('aws_secret_access_key'),
        'region_name': os.getenv('region_name'),
        'bucket': "miguelsiloli-projects-s3",
        's3_folder': "housing_prices/raw/imovirtual/"
    },
    dag=dag,
)

fetch_data >> process_and_upload