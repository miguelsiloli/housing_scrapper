import pandas as pd
import os
import json
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv
import boto3
import io
import os
from b2sdk.v2 import B2Api

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

# def upload_df_to_backblaze(df, file_name, folder_name):
#     """
#     Uploads a pandas DataFrame to Backblaze B2 storage within a specified folder.
#     Creates the folder if it does not exist.

#     Args:
#         df (pandas.DataFrame): The DataFrame to upload
#         file_name (str): Name to give the file in B2 (should end in .csv)
#         folder_name (str): Name of the folder in B2 where the file should be stored
    
#     Returns:
#         bool: True if upload was successful, False otherwise
        
#     Raises:
#         Exception: If there's an error during upload
#     """

#     # Convert DataFrame to CSV in memory
#     csv_buffer = io.StringIO()
#     df.to_csv(csv_buffer, index=False)
#     csv_data = csv_buffer.getvalue().encode('utf-8')
    
#     # Setup B2 client
#     b2_api = B2Api()
#     b2_api.authorize_account("production", os.getenv("B2_KEY_ID"), os.getenv("B2_APPLICATION_KEY"))
    
#     # Prepare upload path and metadata
#     remote_path = f"{folder_name}/{file_name}"
#     bucket = b2_api.get_bucket_by_name(os.getenv("B2_BUCKET"))
#     file_info = {'Content-Type': 'text/csv'}
    
#     try:
#         # Upload data directly from memory
#         bucket.upload_bytes(
#             data_bytes=csv_data,
#             file_name=remote_path,
#             file_info=file_info
#         )
#         return True
#     except Exception as e:
#         print(f"Error uploading to B2: {str(e)}")
#         return False

def process_and_upload_to_b2(source_path, **context):
    """
    Loads data from source path into a DataFrame and uploads to Backblaze B2.
    
    Args:
        source_path (str): Path to source data
        **context: Airflow context and additional parameters
    """
    import pandas as pd
    import os
    from glob import glob
    
    # Get all CSV files in the source directory
    csv_files = glob(os.path.join(source_path, "*.csv"))
    
    for file_path in csv_files:
        # Read the CSV into a DataFrame
        df = pd.read_csv(file_path)
        
        # Get the original filename
        file_name = os.path.basename(file_path)
        
        # Upload to Backblaze B2
        success = upload_df_to_backblaze(
            df=df,
            file_name=file_name,
            folder_name="housing_prices/raw/imovirtual"
        )
        
        if not success:
            raise Exception(f"Failed to upload {file_name} to Backblaze B2")

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
