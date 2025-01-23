from datetime import datetime
import os
from imovirtual.graphql_main import fetch_imovirtual_data, get_buildid
from imovirtual.parse_data import process_and_upload_to_b2

def main():
    # Define the paths and headers
    csv_file_path = 'imovirtual_catalog.csv'
    output_dir = 'raw/imovirtual/'
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
    base_url_template = "https://www.imovirtual.com/_next/data/{}/pt/resultados/arrendar/apartamento/{}.json"

    # Fetch data from imovirtual
    print("Fetching data from imovirtual...")
    fetch_imovirtual_data(
        csv_file_path=csv_file_path,
        output_dir=output_dir,
        headers=headers,
        base_url_template=base_url_template,
        get_buildid=get_buildid
    )

    # Process and upload data to B2
    print("Processing and uploading data to B2...")
    process_and_upload_to_b2(source_path=output_dir)

    print("Pipeline completed successfully.")

if __name__ == "__main__":
    main()
