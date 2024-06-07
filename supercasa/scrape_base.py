import requests
from lxml import html
import pandas as pd
from time import sleep
import logging


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


headers = {
    'Host': 'supercasa.pt',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'pt-PT,pt;q=0.8,en;q=0.5,en-US;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Referer': 'https://supercasa.pt/arrendar-casas/',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Connection': 'keep-alive',
}

def get_municipios_links(district_url):
    logging.info(f"Fetching municipios links from {district_url}")
    response = requests.get(district_url, headers=headers)
    tree = html.fromstring(response.content)
    municipios_links = tree.xpath('//a[contains(@href, "arrendar-casas")]/@href')
    municipios_links = [
        link for link in municipios_links 
        if 'zona' not in link.split('/')[0] and 'arrendar-casas' not in link.split('/')[0]
    ]
    logging.info(f"Found {len(municipios_links)} municipios links")
    return municipios_links

def parse_municipio(municipio_url):
    response = requests.get(municipio_url, headers=headers)
    tree = html.fromstring(response.content)
    elements = tree.xpath('//a[@data-id and @data-layer]')
    data = []
    for el in elements:
        data_id = el.get('data-id')
        data_layer = el.get('data-layer')
        href = el.get('href')
        value = el.text_content().strip()
        data.append({
            'data_id': data_id,
            'data_layer': data_layer,
            'href': href,
            'value': value
        })
    return data

districts = [
    "aveiro",
    "beja",
    "braga",
    "castelo-branco",
    "coimbra",
    "evora",
    "faro",
    "guarda",
    "leiria",
    "lisboa",
    "portalegre",
    "porto",
    "santarem",
    "setubal",
    "viana-do-castelo",
    "vila-real",
    "viseu"
]

base_url = "https://supercasa.pt/arrendar-casas/"

district_urls = [f"{base_url}{district}/zonas" for district in districts]
all_data = []

for district_url in district_urls:
    municipios_links = get_municipios_links(district_url)

    
    for municipio_link in municipios_links:
        municipio_url = f'https://supercasa.pt{municipio_link}'
        municipio_data = parse_municipio(municipio_url)
        all_data.extend(municipio_data)
        sleep(0.5)

    df = pd.DataFrame(all_data)
    df.drop_duplicates(inplace= True)
    df.to_csv("supercasa_catalog.csv", index= False)