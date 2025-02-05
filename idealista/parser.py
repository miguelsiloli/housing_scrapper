import pandas as pd
import regex as re
import os
import dask
import dask.dataframe as dd
from dask.diagnostics import ProgressBar
from datetime import datetime
from typing import List, Generator, Dict, Optional
from idealista.data_validation import schema
import numpy as np
import boto3
from io import BytesIO
import os
from pathlib import Path
from dotenv import load_dotenv
from lxml import html
from b2sdk.v2 import B2Api, InMemoryAccountInfo
from dotenv import load_dotenv

load_dotenv()

async def read_html_files_async(directory_path):
    """
    Asynchronously read all HTML files from a specified directory.
    
    Args:
    directory_path (str): Path to the directory containing HTML files.

    Returns:
    List[str]: A list of paths to HTML files.
    """
    return [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.html')]


def chunk_files(file_list: List[str], chunk_size: int) -> Generator[List[str], None, None]:
    """
    Generate consecutive chunks of files from a list, with each chunk containing a maximum number of files.
    
    Args:
    file_list (List[str]): The list of file paths.
    chunk_size (int): The maximum number of files per chunk.

    Returns:
    Generator[List[str], None, None]: A generator yielding chunks of file paths.
    """
    for i in range(0, len(file_list), chunk_size):
        yield file_list[i:i + chunk_size]

def parse_html_files_to_dataframe(file_path: str) -> pd.DataFrame:
    """
    Parse multiple HTML files into a pandas DataFrame.
    
    Args:
    file_paths (List[str]): List of paths to HTML files.

    Returns:
    pd.DataFrame: A DataFrame containing data extracted from HTML files.
    """
    listings = []

    # Read and parse each HTML file
    with open(file_path, 'r', encoding='utf-8') as file:
        parsed_html = html.parse(file)

    # Extract articles that represent real estate listings
    articles = parsed_html.xpath('//article[contains(@class, "item") or contains(@class, "extended-item")]')
    for article in articles:
        listing_info = extract_listing_info(article)
        listings.append(listing_info)

    # Create a DataFrame from all collected listings
    df = pd.DataFrame(listings)

    # adding date based on file metadata
    df['date'] = convert_timestamp_to_date(os.stat(file_path).st_ctime)
    df['source_link'] = find_canonical_href(parsed_html)

    if not df.empty:
        df = process_dataframe(df)
    return df

def find_canonical_href(html_element):
    try:
        link = html_element.xpath('//link[@rel="canonical"]/@href')[0]
        return link
    
    except:
        return []


def find_city(article):
    
    # Use XPath to find the specific <span> element with the desired attributes and extract its text
    target_spans = article.xpath("//span[@role='button' and @class='breadcrumb-navigation-current-level arrow-down' and @itemprop='name']/text()")

    # Check if the target span was found and return its text, otherwise return a default message
    return target_spans[0] if target_spans else "Element not found"

def extract_listing_info(article: html.HtmlElement) -> Dict[str, Optional[str]]:
    """
    Extract relevant information from an HTML article element and organize it into a dictionary.
    
    Args:
    article (html.HtmlElement): The HTML element representing an article.

    Returns:
    Dict[str, Optional[str]]: A dictionary containing extracted information such as title, link, and description.
    """
    
    # Extracts data from an article and returns a dictionary of listing info
    # careful with paths, might change between runs need to evaluate this
    title = article.xpath('.//a[@class="item-link "]/@title')[0] if article.xpath('.//a[@class="item-link "]/@title') else np.NaN
    if title == np.NaN:
        return {}
    
    else:
        return {
            'title': title,
            'link': 'https://www.idealista.pt' + str((article.xpath('.//a[@class="item-link "]/@href')[0]) if article.xpath('.//a[@class="item-link "]/@href') else np.NaN),
            'description': article.xpath('.//div[@class="item-description description"]/p[@class="ellipsis"]//text()')[0].strip() if article.xpath('.//div[@class="item-description description"]/p[@class="ellipsis"]//text()') else np.NaN,
            'garage': "True" if article.xpath('.//span[@class="item-parking"]') else "False",
            'price': extract_price(article.xpath('.//span[@class="item-price h2-simulated"]/text()')),
            'additional_details': article.xpath('.//span[@class="item-detail"]/text()'),
            'home_type': str(title).split()[0] if title and title != np.NaN else np.NaN,
            # 'city': find_city(article)
        }

def extract_price(price_texts: List[str]) -> Optional[int]:
    """
    Extract and convert the price text into an integer.
    
    Args:
    price_texts (List[str]): List of price text entries.

    Returns:
    Optional[int]: The price converted into an integer, or None if not available.
    """
    if price_texts:
        numeric_price = re.findall(r'\d+', price_texts[0].replace('.', ''))
        return int(numeric_price[0]) if numeric_price else None
    return None

def extract_last_substring(input_string):
    # Split the string by comma
    parts = input_string.split(',')
    # Return the last part, strip() to remove any leading/trailing whitespace
    return parts[-1].strip()

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process the DataFrame to format and clean the data appropriately.
    
    Args:
    df (pd.DataFrame): The DataFrame to be processed.

    Returns:
    pd.DataFrame: The processed DataFrame.
    """
    df['home_size'] = df['additional_details'].apply(lambda x: x[0] if x else None)
    df['home_area'] = df['additional_details'].apply(lambda x: int(re.search(r'\d+', x[1]).group()) if len(x) >= 2 else None)
    df['floor'] = df['additional_details'].apply(lambda x: x[2] if len(x) >= 3 and 'andar' in x[2] else None)
    df['floor'] = df['floor'].apply(lambda x: int(re.search(r'\d+', str(x)).group()) if x else 0)    
    df['elevator'] = df['floor'].apply(lambda x: 'com elevador' in str(x) if str(x) else False)
    df['price_per_sqr_meter'] = df['price'] / df['home_area']
    # df['neighborhood'] = df['title'].str.rsplit(',', n = 1).str[-1].str.strip()
    df.drop(columns=['additional_details'], 
            inplace=True)
    df.fillna({'elevator': False, 'floor': 0}, 
              inplace=True)

    return df

def main_function(parsing_function: callable, source_directory_path: str = "./raw/idealista", chunk_size: int = 25, upload_to_s3 = False, folder = "housing_prices/raw/idealista") -> None:
    """
    Process HTML files into a DataFrame and save it as a Parquet file.
    
    Args:
    parsing_function (callable): The function to use for parsing individual HTML files.
    directory_path (str): Path to the directory containing HTML files.
    chunk_size (int): Number of files to process at a time.
    """
    # html_files = read_html_files_async(source_directory_path)
    html_files = [os.path.join(source_directory_path, f) for f in os.listdir(source_directory_path) if f.endswith('.html')]

    # Create chunks of HTML files
    file_chunks = list(chunk_files(html_files, chunk_size))

    # Use Dask to parse HTML files in batches
    delayed_dataframes = [dask.delayed(process_html_files)(parsing_function, chunk) for chunk in file_chunks]
    with ProgressBar():
        computed_dataframes = dask.compute(*delayed_dataframes, 
                                            scheduler='processes')

    # Filter out None results from batches
    filtered_dataframes = [df for df in computed_dataframes if df is not None]

    # Concatenate dataframes efficiently
    if filtered_dataframes:
        final_df = pd.concat(filtered_dataframes, 
                             axis=0, 
                             ignore_index=True)
        current_date = final_df["date"].iloc[0]
        filename = f'{folder}house_price_data_{current_date}.parquet'

        # making sure data types match
        final_df = assert_dataframe_datatypes(final_df)

        # dropping duplicates if there are any
        final_df.drop_duplicates(subset=["link"], 
                                  inplace = True)

        # validate dataframe
        # final_df = schema.validate(final_df)

        districts = pd.read_csv("district_data_updated.csv")
        districts["neighborhood_link"] = districts["neighborhood_link"].astype(str)
        final_df["source_link"] = final_df["source_link"].astype(str)
        merged_df = pd.merge(districts, final_df, left_on='neighborhood_link', right_on='source_link', how='right')

        # merged_df.to_parquet(f'house_price_data_{current_date}.parquet')
        merged_df.drop(["Unnamed: 0"], axis = 1, inplace = True)
        # maybe it would be easier to pass args as list/dict
        # bucket name cannot contain underscores
        if upload_to_s3:
            upload_df_to_b2_as_parquet(df = merged_df,
                                       bucket_name = "housing",
                                       file_name = filename,
                                       b2_key_id=os.environ["B2_KEY_ID"],
                                       b2_application_key=os.environ["B2_APPLICATION_KEY"])
            
            # upload_df_to_s3_as_parquet(df = merged_df,
            #                         bucket = "miguelsiloli-projects-s3",
            #                         file_name = filename,
            #                         s3_client = s3_object)
        
def convert_timestamp_to_date(timestamp):
    # Convert timestamp to datetime
    date_object = datetime.fromtimestamp(timestamp)
    # Format datetime as 'dd-mm-yyyy'
    formatted_date = date_object.strftime('%d-%m-%Y')
    return formatted_date

def upload_df_to_b2_as_parquet(df, bucket_name, file_name, b2_key_id, b2_application_key):
    """
    Uploads a DataFrame to a Backblaze B2 bucket as a Parquet file.

    Parameters:
    df (pd.DataFrame): DataFrame to upload
    bucket_name (str): B2 bucket name
    file_name (str): Desired name for the file in B2
    b2_key_id (str): Backblaze B2 key ID
    b2_application_key (str): Backblaze B2 application key

    Returns:
    bool: True if upload was successful

    Raises:
    Exception: If authorization or upload fails
    """
    try:
        # Convert DataFrame to Parquet using BytesIO as an intermediate buffer
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, engine='pyarrow')
        buffer.seek(0)

        # Initialize B2 API
        info = InMemoryAccountInfo()
        b2_api = B2Api(info)
        b2_api.authorize_account("production", b2_key_id, b2_application_key)
        
        # Get bucket
        bucket = b2_api.get_bucket_by_name(bucket_name)
        
        # Upload the file
        bucket.upload_bytes(
            data_bytes=buffer.getvalue(),
            file_name=file_name,
            file_info={'Content-Type': 'application/parquet'}
        )
        
        print(f"File uploaded successfully to B2 bucket '{bucket_name}' as '{file_name}'")
        return True
        
    except Exception as e:
        print(f"Error uploading file to B2: {str(e)}")
        raise

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

def assert_dataframe_datatypes(df):

    # Convert data types
    df['title'] = df['title'].astype('string')  # Using Pandas' new StringDtype
    df['link'] = df['link'].astype('string')
    df['description'] = df['description'].astype('string')
    df['garage'] = df['garage'].astype('bool')
    # df['price'] = df['price'].astype(np.int32)  # Use np.int32 if the price range is within the 32-bit integer range
    df['home_size'] = df['home_size'].astype('string')  
    # df['home_area'] = df['home_area'].astype(np.int32)
    # df['floor'] = df['floor'].astype(np.int32)
    df['elevator'] = df['elevator'].astype('bool')
    # df['price_per_sqr_meter'] = df['price_per_sqr_meter'].astype(np.float32) 
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y')  

    # Validate the new data types
    return(df)
        
def process_html_files(parsing_function: callable, files: List[str]) -> Optional[pd.DataFrame]:
    """
    Process a batch of HTML files using the specified parsing function and return a combined DataFrame.
    
    Args:
    parsing_function (callable): Function to parse individual HTML files.
    files (List[str]): List of file paths.

    Returns:
    Optional[pd.DataFrame]: A DataFrame combined from processed files, or None if no data was processed.
    """
    dataframes = []
    for file in files:
        df = parsing_function(file)
        if df is not None and not df.empty:
            dataframes.append(df)
    if dataframes:
        return pd.concat(dataframes, axis=0, ignore_index=True)
    
    return None


if __name__ == "__main__":
    load_dotenv()
    s3 = boto3.client('s3',
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key'),
            region_name=os.getenv('region_name'))
    
    main_function(parsing_function = parse_html_files_to_dataframe, 
                  s3_object= s3,
                  source_directory_path = "./raw/idealista",
                  upload_to_s3= True)
    
