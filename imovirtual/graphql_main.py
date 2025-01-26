import logging
import requests
import pandas as pd
import json
import os
import time
from tqdm import tqdm
from functools import wraps

def retry_on_failure(retries=3, delay=60):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = retries
            while attempts > 0:
                try:
                    return func(*args, **kwargs)
                except requests.RequestException as e:
                    attempts -= 1
                    print(
                        f"Request failed: {e}. Retrying in {delay} seconds...")
                    time.sleep(delay)
            raise Exception(
                f"Failed to complete {func.__name__} after {retries} retries.")

        return wrapper

    return decorator

@retry_on_failure(retries=3, delay=60)
def make_api_call(url: str, headers: dict) -> dict:
   try:
       response = requests.get(url, headers=headers)
       response.raise_for_status()
       data = response.json()
       time.sleep(2)
       return data
   except Exception as e:
       logging.error(f'Error in API call to {url}: {str(e)}')
       raise

def save_district_data(district: str, data: dict, output_dir: str) -> None:
   output_path = os.path.join(output_dir, f'{district}.json')
   os.makedirs(os.path.dirname(output_path), exist_ok=True)
   
   try:
       with open(output_path, 'w', encoding='utf-8') as f:
           json.dump(data, f, ensure_ascii=False, indent=4)
       logging.info(f'Successfully saved data for district {district}')
   except Exception as e:
       logging.error(f'Error saving data for district {district}: {str(e)}')

def process_district(district: str, id_list: list, base_url_template: str, headers: dict, buildid: str, output_dir: str) -> None:
   all_data = {}
   
   for id in tqdm(id_list, desc=f'Processing IDs for {district}', leave=False):
       url = base_url_template.format(buildid, id)
       
       try:
           data = make_api_call(url, headers)
           all_data[id] = data["pageProps"]['data']['searchAds']['items']
           num_pages = data["pageProps"]['tracking']['listing']['page_count']
           
           if num_pages > 1:
               for page in tqdm(range(2, num_pages + 1), desc='Fetching pages', leave=False):
                   paged_url = f"{url}?page={page}"
                   paged_data = make_api_call(paged_url, headers)
                   all_data[id].extend(paged_data["pageProps"]['data']['searchAds']['items'])
                   
           logging.info(f'Successfully processed ID {id} with {num_pages} pages')
           
       except Exception as e:
           logging.error(f'Error processing ID {id}: {str(e)}')
           continue

   save_district_data(district, all_data, output_dir)

def fetch_imovirtual_data(csv_file_path: str, output_dir: str, headers: dict, base_url_template: str, get_buildid) -> None:
   logging.basicConfig(
       level=logging.INFO,
       format='%(asctime)s - %(levelname)s - %(message)s',
       handlers=[
           logging.FileHandler('imovirtual_scraping.log'),
           logging.StreamHandler()
       ]
   )
   df = pd.read_csv(csv_file_path)
   districts = df.groupby("district")['id'].apply(list).to_dict()
   buildid = get_buildid()
   
   logging.info(f'Starting data collection for {len(districts)} districts')
   
   for district, id_list in tqdm(districts.items(), desc='Processing districts'):
       logging.info(f'Processing district: {district}')
       process_district(district, id_list, base_url_template, headers, buildid, output_dir)