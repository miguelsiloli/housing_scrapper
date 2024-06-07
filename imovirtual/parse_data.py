import pandas as pd
import os
import json
from datetime import datetime

# Function to flatten the JSON data and add a location column
def flatten_json(data, location):
    all_data = []
    for item in data:
        flat_item = pd.json_normalize(item, sep='_')
        flat_item['location'] = location
        all_data.append(flat_item)
    return all_data

def convert_timestamp_to_date(timestamp):
    # Convert timestamp to datetime
    date_object = datetime.fromtimestamp(timestamp)
    # Format datetime as 'dd-mm-yyyy'
    formatted_date = date_object.strftime('%d-%m-%Y')
    return formatted_date


if __name__ == "__main__":
    source_path = "raw/imovirtual/"

    files = [pos_json for pos_json in os.listdir(source_path) if pos_json.endswith('.json')]

    # Flatten and process the data
    all_data = []

    for file in files:
        file_path = str(source_path + file)
        print(file_path)
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        for key, items in data.items():
            all_data.extend(flatten_json(items, key))

        df_flat = pd.concat(all_data, ignore_index=True)
        df_flat['date'] = convert_timestamp_to_date(os.stat(file_path).st_ctime)

        # Dropping columns containing '__typename' and 'images'
        df_flat = df_flat[[col for col in df_flat.columns if '__typename' not in col and 'images' not in col]]

    df_flat.to_parquet("imovirtual_processed.parquet")
