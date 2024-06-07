import requests
import pandas as pd
import json
import time
from get_buildid import get_buildid

# Define the headers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Accept": "multipart/mixed, application/graphql-response+json, application/graphql+json, application/json",
    "Accept-Language": "pt-PT,pt;q=0.8,en;q=0.5,en-US;q=0.3",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Content-Type": "application/json",
    "baggage": "sentry-environment=imovirtualpt2-prd,sentry-release=frontend-platform%40v20240603T121501-imovirtualpt2,sentry-public_key=feffe528c390ea66992a4a05131c3c68,sentry-trace_id=cc8c6f78b99b4e959c670ca3a2b379bd,sentry-transaction=%2Fpt%2Fresultados%2F%5B%5B...searchingCriteria%5D%5D,sentry-sampled=false",
    "Origin": "https://www.imovirtual.com",
    "Alt-Used": "www.imovirtual.com",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "Priority": "u=1",
    "TE": "trailers"
}

# Read CSV to get the list of districts
df = pd.read_csv('imovirtual_catalog.csv')
districts = df.groupby("district")['id'].apply(list)

for id_list in districts:
    for id in id_list:
        print(id)
    break

buildid = get_buildid()

base_url = "https://www.imovirtual.com/_next/data/{}/pt/resultados/arrendar/apartamento/{}.json"


for district, id_list in districts.items():

    # Initialize an empty dictionary to store the data    
    all_data = {}    
    for id in id_list:
        url = base_url.format(buildid, id)
        response = requests.get(url, headers= headers)
        data = response.json()
        all_data[id] = data["pageProps"]['data']['searchAds']['items']
        
        num_pages = data["pageProps"]['tracking']['listing']['page_count']
        
        for page in range(2, num_pages + 1):
            paged_url = url + f"?page={page}"
            paged_response = requests.get(paged_url, headers= headers)
            paged_data = paged_response.json()
            all_data[id].extend(paged_data["pageProps"]['data']['searchAds']['items'])
            
            # To prevent being blocked by the server for too many requests in a short time
            time.sleep(12)

    # Save the collected data to a JSON file
    with open(f'raw/imovirtual/{district}.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)