from datetime import datetime, timedelta
from idealista.scraper_base import scraper_factory 
from idealista.parser import main_function, parse_html_files_to_dataframe
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

def run_pipeline():
    try:
        # Step 1: Scraping
        scrape_idealista(urls=idealista_urls, 
                        directory_path='raw/idealista')
        
        # Step 2: Parsing and uploading to S3
        main_function(parsing_function=parse_html_files_to_dataframe,
                     source_directory_path="./raw/idealista",
                     upload_to_s3 = False)
        
        # Step 3: Cleanup
        delete_folder("./raw/idealista")
        
        print(f"Pipeline completed successfully at {datetime.now()}")
        
    except Exception as e:
        print(f"Error in pipeline: {str(e)}")
        # You could add email notification here

if __name__ == "__main__":
    run_pipeline()
