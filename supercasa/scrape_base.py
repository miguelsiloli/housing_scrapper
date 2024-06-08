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
