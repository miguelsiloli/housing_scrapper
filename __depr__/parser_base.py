import pandas as pd
from bs4 import BeautifulSoup
import regex as re
import os
import dask
import dask.dataframe as dd
from idealista.notebooks.duckdb_handler import DatabaseConnection
from dask.diagnostics import ProgressBar

def find_city(html_source):
    soup = BeautifulSoup(html_source, 'lxml')

    # Find the specific <span> element with the desired attributes and extract its text
    target_span = soup.find('span', {'role': 'button', 'class': 'breadcrumb-navigation-current-level arrow-down', 'itemprop': 'name'})
    return target_span.text if target_span else "Element not found"

def parse_idealista(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
        soup = BeautifulSoup(html_content, 
                             'html.parser')
        listings = []

        # Extract each article which represents a real estate listing
        articles = soup.find_all('article', class_=['item', 'extended-item'])
        for article in articles:
            # Initialize dictionary to store listing details
            listing_info = {}
            
            # Extracting the title and link of the property
            title_link = article.find('a', class_='item-link')
            if title_link:
                listing_info['title'] = title_link.get('title', '')
                # Split the title by space and take the first word as home_type
                title_words = listing_info['title'].split()
                if title_words:  # Check if there is at least one word
                    listing_info['home_type'] = title_words[0]
                else:
                    listing_info['home_type'] = ''  # Handle cases where the title might be empty
                listing_info['link'] = 'https://www.idealista.pt' + title_link.get('href', '')

            # Extracting price information
            price_info = article.find('span', class_='item-price')
            parking_info = article.find('span', class_='item-parking')
            if parking_info:
                listing_info['garage'] = "Yes"
            else:
                listing_info['garage'] = "No"
            if price_info:
                numeric_price = re.findall(r'\d+\.\d+|\d+', price_info.text.strip())
                if numeric_price:
                    listing_info['price'] = numeric_price[0]  

            # Extracting detail characters like 'T1', '55 m² área bruta', etc.
            details = article.find_all('span', class_='item-detail')
            details_text = [detail.get_text(strip=True) for detail in details]
            listing_info["additional_details"] = details_text
            description = article.find('div', class_="item-description description").text.strip()
            listing_info['description'] = description

            # Adding the listing to the list
            listings.append(listing_info)

        # Creating a DataFrame
        df = pd.DataFrame(listings)

        # Initialize the new columns
        df['home_size'] = None
        df['home_area'] = None
        df['floor'] = None

        # Iterate over rows and fill the columns accordingly
        for idx, row in df.iterrows():
            details = row['additional_details']
            if len(details) >= 2:
                df.at[idx, 'home_size'] = details[0]
                df.at[idx, 'home_area'] = int(re.search(r'\d+', details[1]).group())
                if len(details) >= 3 and 'andar' in details[2]:
                    df.at[idx, 'floor'] = details[2]

        # Drop the original 'additional_details' column
        # df.drop(columns=['additional_details'], inplace=True)
        df['elevator'] = df['floor'].str.contains('com elevador')
        df['floor'] = df['floor'].apply(lambda x: int(re.search(r'\d+', str(x)).group()) if x is not None else None)
        df['price'] = df['price'].str.replace('.', '')
        df['price'] = df['price'].astype(int)
        df['price_per_sqr_meter'] = df['price']/df['home_area']
        df['elevator'] = df['elevator'].fillna(False)
        df['floor'] = df['floor'].fillna(0)
        df.drop("additional_details", axis = 1, inplace = True)
        df["city"] = find_city(html_content)
        df["source"] = "idealista"
        # do this adhoc
        df['neighborhood'] = df['title'].str.rsplit(',', n = 1).str[-1].str.strip()
        return df

def extract_numbers(text):
    # Check if text is not None and is not NaN (using pandas' isna())
    if pd.isna(text):
        return None
    
    # Use regex to find the first occurrence of one or more digits
    match = re.search(r'\d+', str(text))
    if match:
        return match.group()
    else:
        return None

# generic dataframe function
def expand_details_to_columns(dataframe, column_name):
    """
    Expands dictionaries in a specified column of a DataFrame into separate columns.
    
    Args:
        dataframe (pd.DataFrame): The DataFrame containing the column with dictionaries.
        column_name (str): The name of the column that contains the dictionaries to expand.
        
    Returns:
        pd.DataFrame: A DataFrame with the original data and new columns for each key in the dictionary.
    """
    # Ensure the column exists in the DataFrame
    if column_name not in dataframe.columns:
        raise ValueError(f"The column {column_name} does not exist in the DataFrame.")
    
    # Expand the column into new DataFrame columns
    details_df = dataframe[column_name].apply(pd.Series)
    
    # Concatenate the original DataFrame with the new columns
    return pd.concat([dataframe.drop(columns=[column_name]), details_df], axis=1)

def extract_last_substring(input_string):
    # Split the string by comma
    parts = input_string.split(',')
    # Return the last part, strip() to remove any leading/trailing whitespace
    return parts[-1].strip()

def parser_imovirtual(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
        soup = BeautifulSoup(html_content, 
                             'html.parser')
        
        # Find all article tags with the specific class
        articles = soup.find_all('article', class_='css-136g1q2 e88tro00')

        # Initialize a list to store extracted data
        listings = []

        # Loop through each article to extract information
        for article in articles:
            # Extract the link and title from the anchor tag inside the article
            link_tag = article.find('a', {'data-testid': 'listing-item-link'})
            url = link_tag['href'] if link_tag else "No URL"
            title = link_tag.get_text(strip=True) if link_tag else "No Title"

            # Extract the price
            price_tag = article.find('span', class_='css-1uwck7i e1a3ad6s0')
            price = price_tag.get_text(strip=True) if price_tag else "No Price"
            # price = int(float(re.sub(r'[^\d]', '', price)))

            # Extract the address
            address_tag = article.find('p', {'data-testid': 'advert-card-address'})
            address = address_tag.get_text(strip=True) if address_tag else "No Address"

            # Extract details (Tipologia and Zona)
            details_tag = article.find('dl', class_='css-uki0wd e12r8p6s1')
            details = {}
            if details_tag:
                for dt, dd in zip(details_tag.find_all('dt'), details_tag.find_all('dd')):
                    details[dt.get_text(strip=True)] = dd.get_text(strip=True)

            # Add all info to the list
            listings.append({
                'price': price,
                'title': title,
                'link': f'https://www.imovirtual.com{url}',
                'neighborhood': address,
                'details': details,
                'city': '',
                'home_type': '',
                'garage': '',
                'description': '',
                'elevator': ''
            })
        data = pd.DataFrame(listings)
        if data.empty:
            return 
        
        
        # dividing the Details column into Tipologia -> home_type, Zona -> home_size, Andar -> floor
        data = expand_details_to_columns(data, 'details')
        # rename to match data
        data.rename(columns={'Tipologia':'home_size', 
                             'Zona':'home_area', 
                             'Andar': 'floor'}, inplace=True)
        # this isnt a good practice imo
        data["home_area"] = data["home_area"].apply(extract_numbers).astype(float)
        data["price"] = data["price"].apply(extract_numbers).astype(float)
        data["floor"] = data["floor"].apply(extract_numbers).astype(float)
        data["city"] = data["neighborhood"].apply(extract_last_substring)
        data["price_per_sqr_meter"] = data["price"]/data["home_area"]
        data["source"] = "imovirtual"

        return (data)
        
"""
def parser(parsing_function, directory_path = "idealista"):

    # check connection first
    db = DatabaseConnection('house_prices.db')
    # Create a DuckDB table from the Pandas DataFrame
    with db.managed_cursor() as con:

        # List HTML files in the directory
        html_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.html')]

        # Using dask.delayed to parse HTML files asynchronously
        delayed_dataframes = [dask.delayed(parsing_function)(file) for file in html_files]
        computed_dataframes = dask.compute(*delayed_dataframes)

        # need to compute before merging to separate empty dataframes
        filtered_dataframes = [result for result in computed_dataframes if result is not None]

        # dask_df = dd.from_delayed(filtered_dataframes)
        pandas_df = pd.concat(filtered_dataframes, 
                            axis = 0, 
                            ignore_index= True)

        create_table_query = f"CREATE TABLE {directory_path} AS SELECT * FROM pandas_df"
        con.execute(f"DROP TABLE IF EXISTS {directory_path}")
        con.execute(create_table_query)
"""

def parser_2(parsing_function, directory_path="idealista"):
    # Check connection first
    db = DatabaseConnection('house_prices.db')
    with db.managed_cursor() as con:
        # List HTML files in the directory
        html_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.html')]
        
        # Use Dask to parse HTML files asynchronously
        delayed_dataframes = [dask.delayed(parsing_function)(file) for file in html_files]
        with ProgressBar():
            computed_dataframes = dask.compute(*delayed_dataframes, scheduler='processes')

        # Filter out empty dataframes
        filtered_dataframes = [df for df in computed_dataframes if df is not None and not df.empty]

        # Concatenate dataframes efficiently
        if filtered_dataframes:
            pandas_df = pd.concat(filtered_dataframes, axis=0, ignore_index=True)

            # Insert into DuckDB
            con.execute(f"DROP TABLE IF EXISTS {directory_path}")
            pandas_df.to_sql(directory_path, con, index=False, if_exists='replace')

#parser(parser_imovirtual, 
#       directory_path = "imovirtual")
parser_2(parse_idealista, 
       directory_path = "raw/idealista")