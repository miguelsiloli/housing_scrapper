import pandas as pd
import json
from typing import List, Dict
from datetime import datetime, date
from glob import glob
import os
from b2sdk.v2 import B2Api, InMemoryAccountInfo
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

def flatten_json(nested_json: Dict) -> Dict:
    flat_dict = {}
    def flatten(x: Dict, prefix: str = ''):
        if isinstance(x, dict):
            for key, value in x.items():
                if key not in ['__typename', 'images', 'mapDetails']:
                    flatten(value, f"{prefix}{key}_" if prefix else key)
        elif isinstance(x, list) and x and isinstance(x[0], dict):
            if 'fullName' in x[0]:
                flat_dict[f"{prefix}names"] = [item['fullName'] for item in x]
                flat_dict[f"{prefix}ids"] = [item['id'] for item in x]
        else:
            flat_dict[prefix.rstrip('_')] = x
    
    flatten(nested_json)
    return flat_dict

def process_json(data: Dict) -> pd.DataFrame:
    all_listings = []
    for region_listings in data.values():
        if isinstance(region_listings, list):
            all_listings.extend(region_listings)
    
    processed_data = []
    for listing in all_listings:
        flat_listing = flatten_json(listing)
        
        for date_field in ['dateCreated', 'dateCreatedFirst']:
            if flat_listing.get(date_field):
                try:
                    flat_listing[date_field] = pd.to_datetime(flat_listing[date_field])
                except ValueError:
                    flat_listing[date_field] = None
        
        for price_field in ['totalPrice', 'pricePerSquareMeter']:
            if isinstance(flat_listing.get(price_field), dict):
                flat_listing[f"{price_field}_value"] = flat_listing[price_field].get('value')
                flat_listing[f"{price_field}_currency"] = flat_listing[price_field].get('currency')
                del flat_listing[price_field]
        
        processed_data.append(flat_listing)
    
    df = pd.DataFrame(processed_data)
    df.columns = [col.replace('location_reverseGeocoding_locations_', 'location_') for col in df.columns]
    return df

def read_and_process_json(directory: str) -> pd.DataFrame:
    all_dfs = []
    
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if data:
                    df = process_json(data)
                    all_dfs.append(df)
                else:
                    logger.warning(f"Empty JSON file: {filename}")
            except Exception as e:
                logger.error(f"Error processing {filename}: {str(e)}")
                continue
    
    if not all_dfs:
        logger.error("No valid data to process")
        return pd.DataFrame()
    
    return pd.concat(all_dfs, ignore_index=True)

def upload_to_b2(source_path: str) -> bool:
    df = read_and_process_json(source_path)
    if df.empty:
        return False
    
    today = date.today().strftime("%Y%m%d")
    parquet_name = f"imovirtual_{today}.parquet"
    df.to_parquet(parquet_name)
    

    info = InMemoryAccountInfo()
    b2_api = B2Api(info)
    b2_api.authorize_account("production", 
                           os.environ["B2_KEY_ID"], 
                           os.environ["B2_APPLICATION_KEY"])
    
    bucket = b2_api.get_bucket_by_name(os.environ["B2_BUCKET_NAME"])
    bucket.upload_local_file(
        local_file=parquet_name,
        file_name=f"imovirtual/{parquet_name}",
        file_info={'Content-Type': 'application/parquet'}
    )
    logger.info(f"Uploaded {parquet_name} to B2")
    os.remove(parquet_name)
    return True
