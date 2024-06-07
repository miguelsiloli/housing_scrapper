import re
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from seleniumbase import Driver
import pandas as pd
from time import sleep

ran_through_urls = []

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def setup_driver() -> webdriver.Chrome:
    """Setup the Selenium WebDriver with necessary options.

    Returns:
        webdriver.Chrome: Configured WebDriver instance.
    """
    driver = Driver(uc=True, incognito=True)
    return driver

def fetch_page_content(driver: webdriver.Chrome, url: str) -> str:
    try:
        driver.get(url)
        logging.info(f"Successfully fetched content from {url}")
        return driver.page_source
    except Exception as e:
        logging.error(f"Error fetching page content: {e}")
        return ""

def extract_links_from_sublocations(page_content: str) -> list:
    sleep(2)
    soup = BeautifulSoup(page_content, 'html.parser')
    sublocations_list = soup.find(id='sublocations-list')
    if not sublocations_list:
        return []
    links = [a['href'] for a in sublocations_list.find_all('a', href=True)]
    return links

def parse_neighborhoods(driver, parish_url):
    page_source = fetch_page_content(driver, parish_url)
    neighborhoods = extract_links_from_sublocations(page_source)
    neighborhoods = [("https://www.idealista.pt" + url) for url in neighborhoods]
    return neighborhoods

def parse_parishes(driver, municipality_url):
    page_source = fetch_page_content(driver, municipality_url)
    parishes = extract_links_from_sublocations(page_source)
    parishes = [("https://www.idealista.pt" + url) for url in parishes]
    parish_data = {}
    # check if parishes is an empty list
    if parishes:
        for parish_url in parishes:
            neighborhoods = parse_neighborhoods(driver, parish_url)
            parish_data[parish_url] = neighborhoods
    return parish_data

def parse_municipalities(driver, district_url):
    page_source = fetch_page_content(driver, district_url)
    municipalities = extract_links_from_sublocations(page_source)
    municipalities = [("https://www.idealista.pt" + url) for url in municipalities]
    municipality_data = {}
    if municipalities:
        for municipality_url in municipalities:
            parishes = parse_parishes(driver, municipality_url)
            municipality_data[municipality_url] = parishes
    return municipality_data

def parse_district(driver, district_url):
    district_data = parse_municipalities(driver, district_url)
    return district_data

def flatten_json_to_df(district_url, data):
    records = []
    for municipality, parishes in data.items():
        for parish, neighborhoods in parishes.items():
            for neighborhood in neighborhoods:
                records.append({
                    "district": district_url,
                    "municipality": municipality,
                    "parish": parish,
                    "neighborhood": neighborhood
                })
    return pd.DataFrame(records)

def remove_mapa_suffix(url):
    return re.sub(r'/mapa$', '', url)

def extract_location(url):
    match = re.search(r'/([^/]+)/mapa$', url)
    if match:
        return match.group(1)
    return None

# run this async
if __name__ == '__main__':
    driver = setup_driver()
    districts = [
    "https://www.idealista.pt/arrendar-casas/aveiro-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/beja-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/braga-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/castelo-branco-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/coimbra-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/evora-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/faro-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/guarda-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/leiria-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/lisboa-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/portalegre-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/porto-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/santarem-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/setubal-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/viana-do-castelo-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/vila-real-distrito/mapa",
    "https://www.idealista.pt/arrendar-casas/viseu-distrito/mapa"
    ]
    # base_url = "https://www.idealista.pt"
    # initial_page = f"{base_url}/arrendar-casas/porto-distrito/mapa"
    df_list = []
    for page in districts:
        district_data = parse_district(driver, 
                                    page)
        df_list.append(flatten_json_to_df(district_data, district_data))
    df = pd.concat(df_list, axis = 0, ignore_index = True)
    df.to_csv('district_data.csv', index=False)
    df['district'] = df['district'].apply(extract_location)
    df['parish'] = df['parish'].apply(extract_location)
    df['municipality'] = df['municipality'].apply(extract_location)
    df['neighborhood_link'] = df['neighborhood'].apply(remove_mapa_suffix)
    df['neighborhood'] = df['neighborhood'].apply(extract_location)
    df.to_csv('district_data.csv', index=False)
