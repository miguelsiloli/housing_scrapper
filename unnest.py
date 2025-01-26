import pandas as pd
import json
from typing import List, Dict
from datetime import datetime
import os

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
    """Process JSON data into a pandas DataFrame with flattened structure"""
    # Extract listings from each region
    all_listings = []
    for region_listings in data.values():
        if isinstance(region_listings, list):
            all_listings.extend(region_listings)
    
    processed_data = []
    for listing in all_listings:
        flat_listing = flatten_json(listing)
        
        # Convert dates
        for date_field in ['dateCreated', 'dateCreatedFirst']:
            if flat_listing.get(date_field):
                try:
                    flat_listing[date_field] = pd.to_datetime(flat_listing[date_field])
                except ValueError:
                    flat_listing[date_field] = None
        
        # Extract nested price values
        for price_field in ['totalPrice', 'pricePerSquareMeter']:
            if isinstance(flat_listing.get(price_field), dict):
                flat_listing[f"{price_field}_value"] = flat_listing[price_field].get('value')
                flat_listing[f"{price_field}_currency"] = flat_listing[price_field].get('currency')
                del flat_listing[price_field]
        
        processed_data.append(flat_listing)
    
    df = pd.DataFrame(processed_data)
    df.columns = [col.replace('location_reverseGeocoding_locations_', 'location_') for col in df.columns]
    return df

def read_json_files(directory: str) -> Dict:
    all_data = {}
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if not data:  # Check if empty
                        print(f"Error: Empty JSON file detected: {filename}")
                        continue
                    all_data[filename] = data
            except json.JSONDecodeError:
                print(f"Error: Invalid JSON format in file: {filename}")
            except Exception as e:
                print(f"Error reading {filename}: {str(e)}")
    return all_data

# [Previous flatten_json and process_json functions remain unchanged]

def main():
    data = read_json_files('raw/imovirtual')
    if not data:
        print("Error: No valid JSON files found")
        return None
        
    all_dfs = []
    for filename, json_data in data.items():
        try:
            df = process_json(json_data)
            all_dfs.append(df)
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")
            continue
    
    if not all_dfs:
        print("Error: No data could be processed")
        return None
        
    final_df = pd.concat(all_dfs, ignore_index=True)
    final_df.to_csv("processed_json.csv", index=False)
    print(f"Total listings: {len(final_df)}")
    print("\nColumns:")
    print(final_df.columns.tolist())
    return final_df

if __name__ == "__main__":
    df = main()