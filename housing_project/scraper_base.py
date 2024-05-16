"""
+----------------------+
| <<abstract>>         |
| WebScraper           |
+----------------------+
| - driver             |
| - options            |
+----------------------+
| + setup_driver()     |
| + scrape()           |
| + save_data()        |
+----------------------+
          ^
          |
+----------------------+
| IdealistaScraper     |
+----------------------+
| - base_url           |
+----------------------+
| + setup_driver()     |
| + scrape()           |
| + save_data()        |
| + find_total_pages() |
| + modify_link()      |
+----------------------+

"""

from abc import ABC, abstractmethod
import undetected_chromedriver as uc
from selenium.webdriver.chrome.options import Options
import re
from datetime import datetime
from time import sleep
import random
import os
from bs4 import BeautifulSoup
from typing import List
from tqdm import tqdm
import numpy as np
from seleniumbase import Driver

class WebScraper(ABC):
    def __init__(self):
        self.driver = None
        self.options = Options()

    @abstractmethod
    def setup_driver(self):
        pass

    @abstractmethod
    def scrape(self):
        pass

    @abstractmethod
    def save_data(self, source_html, filename):
        pass


class IdealistaScraper(WebScraper):
    def __init__(self):
        super().__init__()

    def setup_driver(self) -> None:
        # Configure options specific to Idealista
        custom_width = 360
        custom_height = 360
        self.options.add_argument(f"--window-size={custom_width},{custom_height}")
        # self.options.add_argument('--ignore-certificate-errors')
        # self.options.add_argument("--headless")
        self.driver = Driver(uc=True, incognito=True)

    def scrape(self, urls: List[str], directory_path: str = "raw/idealista") -> None:
        """Scrape data from a list of URLs using Selenium WebDriver."""
        try:
            self.setup_driver()
            for base_url in tqdm(urls):
                self.driver.get(base_url)
                self.total_pages = self.find_total_pages(self.driver.page_source)
                self.save_data(self.driver.page_source, base_url, directory_path)

                for page in range(2, self.total_pages + 1):
                    print(f"Fetching {page} of {self.total_pages}\n URL: {base_url}")
                    sleep(np.abs(random.normalvariate(10, 2)))  # Delay to avoid being blocked by the server

                    # additional sleep every 5 pages
                    if page % 5 == 0:
                        print(f"Extra sleep at page {page}.")
                        sleep(np.abs(random.normalvariate(10, 2)))

                    current_page_url = f"{base_url}pagina-{page}"
                    self.driver.get(current_page_url)
                    self.save_data(self.driver.page_source, current_page_url)

                sleep(np.abs(random.normalvariate(40, 10)))
        finally:
            if self.driver:
                self.driver.quit()

    def save_data(self, source_html: str, filename: str, directory_path: str) -> None:
        """Save the source HTML into a file within the 'idealista' directory."""
        
        # Ensure the directory exists (creates if it doesn't)
        os.makedirs(directory_path, exist_ok=True)
        
        # Construct the full file path
        full_file_path = os.path.join(directory_path, self.modify_link(filename))
        
        # Write the source HTML to the specified file
        with open(full_file_path, "w", encoding="utf-8") as file:
            file.write(source_html)

    @staticmethod
    def find_total_pages(html_source: str) -> int:
        """Extract the total number of pages from the HTML source using a regular expression."""
        # i have to take a function as param since find total pages is different from page to page
        total_pages = re.search(r'"totalPageNumber":"(\d+)"', html_source)
        return int(total_pages.group(1)) if total_pages else 2

    @staticmethod
    def modify_link(link: str) -> str:
        """Modify the link to create a filename including the current date."""
        pattern = r"arrendar-casas/(.*)"
        match = re.search(pattern, link)
        if match:
            modified_part = match.group(0).replace("/", "+")
            current_date = datetime.now().strftime("%d-%m-%y")
            final_link = f"{modified_part}+{current_date}.html"
            return final_link
        return "Pattern not found in the link."
    
class ImovirtualScraper(WebScraper):
    def __init__(self):
        super().__init__()

    def setup_driver(self):
        # Configure options specific to Idealista
        custom_width = 360
        custom_height = 360
        self.options.add_argument(f"--window-size={custom_width},{custom_height}")
        # self.options.add_argument('--ignore-certificate-errors')
        # self.options.add_argument("--headless")
        self.driver = uc.Chrome(use_subprocess=True, options= self.options)

    def scrape(self, urls: List[str], directory_path: str = "imovirtual") -> None:
        """Scrape data from a list of URLs using Selenium WebDriver."""
        try:
            self.setup_driver()
            for base_url in tqdm(urls):
                self.driver.get(base_url)
                self.total_pages = self.find_total_pages(self.driver.page_source)
                self.save_data(self.driver.page_source, base_url, directory_path)

                for page in range(2, self.total_pages + 1):
                    print(f"Fetching {page} of {self.total_pages}\n URL: {base_url}")
                    sleep(np.abs(random.normalvariate(10, 2)))  # Delay to avoid being blocked by the server

                    # additional sleep every 5 pages
                    if page % 5 == 0:
                        print(f"Extra sleep at page {page}.")
                        sleep(np.abs(random.normalvariate(10, 2)))

                    current_page_url = f"{base_url}pagina-{page}"
                    self.driver.get(current_page_url)
                    self.save_data(self.driver.page_source, current_page_url)

                sleep(np.abs(random.normalvariate(30, 10)))
        finally:
            if self.driver:
                self.driver.quit()

    def save_data(self, source_html: str, filename: str, directory_path: str = "imovirtual") -> None:
        """Save the source HTML into a file within the 'idealista' directory."""
        
        # Ensure the directory exists (creates if it doesn't)
        os.makedirs(directory_path, exist_ok=True)
        
        # Construct the full file path
        full_file_path = os.path.join(directory_path, self.modify_link(filename))
        
        # Write the source HTML to the specified file
        with open(full_file_path, "w", encoding="utf-8") as file:
            file.write(source_html)

    @staticmethod
    def find_total_pages(html_source):
        soup = BeautifulSoup(html_source, 'lxml')
        target_lis = soup.find_all('li', class_='css-1tospdx')

        # this returns every page in the html
        # its convenient to handle text -> float -> int
        total_pages = max([int(float(li.text)) for li in target_lis])
        return total_pages

    @staticmethod
    def modify_link(link):
        pattern = r"arrendar/(.*)"
        match = re.search(pattern, link)
        if match:
            modified_part = match.group(0).replace("/", "+")
            modified_part = modified_part.replace("?", "+")
            current_date = datetime.now().strftime("%d-%m-%y")
            final_link = f"{modified_part}+{current_date}.html"
            return final_link
        return "Pattern not found in the link."
    
def scraper_factory(source: str) -> WebScraper:
    """Factory function to create instances of scrapers based on the given source.
    
    Args:
        source (str): The name of the source for which to create the scraper.

    Returns:
        WebScraper: An instance of the requested scraper.

    Raises:
        ValueError: If the source is not supported.
    """
    factory = {
        'idealista': IdealistaScraper(),
        'imovirtual': ImovirtualScraper(),
    }
    if source in factory:
        return factory[source]
    else:
        raise ValueError(f"source {source} is not supported. Please pass a valid source.")

# Starting URLs for scraping
idealista_urls = [
    "https://www.idealista.pt/arrendar-casas/porto/",
    "https://www.idealista.pt/arrendar-casas/lisboa/",
    "https://www.idealista.pt/arrendar-casas/braga/",
]

idealista_urls = [
    "https://www.idealista.pt/arrendar-casas/aveiro/",
    "https://www.idealista.pt/arrendar-casas/coimbra/",
    "https://www.idealista.pt/arrendar-casas/viseu/",
    "https://www.idealista.pt/arrendar-casas/viana-do-castelo/",
    "https://www.idealista.pt/arrendar-casas/maia/", 
    "https://www.idealista.pt/arrendar-casas/cascais/",
    "https://www.idealista.pt/arrendar-casas/sintra/",
    "https://www.idealista.pt/arrendar-casas/leiria/",
    "https://www.idealista.pt/arrendar-casas/matosinhos/",
    "https://www.idealista.pt/arrendar-casas/vila-nova-de-gaia/",
    "https://www.idealista.pt/arrendar-casas/loures/",
    "https://www.idealista.pt/arrendar-casas/almada/",
    "https://www.idealista.pt/arrendar-casas/setubal/",
    "https://www.idealista.pt/arrendar-casas/guimaraes/",
    "https://www.idealista.pt/arrendar-casas/gondomar/"
]

imovirtual_urls = [
    "https://www.imovirtual.com/pt/resultados/arrendar/apartamento/porto/porto",
    "https://www.imovirtual.com/pt/resultados/arrendar/apartamento/lisboa/lisboa",
    "https://www.imovirtual.com/pt/resultados/arrendar/apartamento/braga/braga"
]

if __name__ == "__main__":
    # Create an instance of the scraper
    idealista_scraper = scraper_factory('idealista')
    idealista_scraper.scrape(urls=idealista_urls,
                            directory_path = 'raw/idealista')
    #imovirtual_scraper = scraper_factory('imovirtual')
    #imovirtual_scraper.scrape(base_urls= imovirtual_urls)
