import pandas as pd
import os
import json
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv
import boto3

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

def upload_df_to_s3_as_parquet(df, bucket, file_name, s3_client):
    """
    Uploads a DataFrame to an S3 bucket as a Parquet file.

    Parameters:
    df (pd.DataFrame): DataFrame to upload.
    bucket (str): S3 bucket name.
    s3_client: s3 object
    """
    # Convert DataFrame to Parquet using BytesIO as an intermediate buffer
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine='pyarrow')

    # Reset buffer position to the start
    buffer.seek(0)

    # im assuming the bucket already exists

    # Upload the Parquet file
    s3_client.upload_fileobj(
        buffer,
        bucket,
        file_name,
        ExtraArgs={'ContentType': 'application/octet-stream'}
    )
    print(f"File uploaded successfully to s3://{bucket}/{file_name}")

def convert_timestamp_to_date(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')

def process_and_upload_data_to_s3(source_path: str, aws_access_key_id: str, aws_secret_access_key: str, region_name: str, bucket: str, s3_folder: str) -> None:
    """
    Process JSON files in a given directory, flatten the data, and upload the result as a Parquet file to S3.

    Parameters:
    - source_path (str): The local directory containing JSON files.
    - aws_access_key_id (str): AWS access key ID.
    - aws_secret_access_key (str): AWS secret access key.
    - region_name (str): AWS region name.
    - bucket (str): S3 bucket name.
    - s3_folder (str): Folder path in the S3 bucket to upload the processed file.
    """

    # Initialize S3 client
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      region_name=region_name)
    
    # List JSON files in the source directory
    files = [pos_json for pos_json in os.listdir(source_path) if pos_json.endswith('.json')]

    all_data = []

    for file in files:
        file_path = os.path.join(source_path, file)
        print(f"Processing file: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        for key, items in data.items():
            all_data.extend(flatten_json(items, key))

    df_flat = pd.concat(all_data, ignore_index=True)
    df_flat['date'] = convert_timestamp_to_date(os.stat(file_path).st_ctime)

    # Dropping columns containing '__typename' and 'images'
    df_flat = df_flat[[col for col in df_flat.columns if '__typename' not in col and 'images' not in col]]

    # Generate the filename for the Parquet file
    current_date = df_flat["date"].iloc[0]
    filename = f'{s3_folder}imovirtual_data_{current_date}.parquet'

    # Upload the DataFrame as a Parquet file to S3
    upload_df_to_s3_as_parquet(df=df_flat, 
                               bucket=bucket, 
                               file_name=filename, 
                               s3_client=s3)



if __name__ == "__main__":
    load_dotenv()

    process_and_upload_data_to_s3(
        source_path="raw/imovirtual/",
        aws_access_key_id=os.getenv('aws_access_key_id'),
        aws_secret_access_key=os.getenv('aws_secret_access_key'),
        region_name=os.getenv('region_name'),
        bucket="miguelsiloli-projects-s3",
        s3_folder="housing_prices/raw/imovirtual/"
    )
