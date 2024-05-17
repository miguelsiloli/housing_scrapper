import re
import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from seleniumbase import Driver
import json

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

def extract_links_from_sublocations(page_content: str, pattern: str) -> list:
    soup = BeautifulSoup(page_content, 'html.parser')
    sublocations_list = soup.find(id='sublocations-list')
    if not sublocations_list:
        return []
    links = [a['href'] for a in sublocations_list.find_all('a', href=True) if re.match(pattern, a['href'])]
    return links

def scrape_links_recursively(driver: webdriver.Chrome, url: str, base_url: str, pattern: str) -> dict:
    """Recursively scrape links from the page and return data in hierarchical JSON format.

    Args:
        driver (webdriver.Chrome): The WebDriver instance.
        url (str): The URL of the page to scrape.
        base_url (str): The base URL of the website.
        pattern (str): The regex pattern to match href links.
        parent_link (str, optional): The parent link to avoid circular references.

    Returns:
        dict: The hierarchical JSON data with links.
    """
    
    global ran_through_urls

    page_content = fetch_page_content(driver, url)
    if not page_content:
        return {}

    links = extract_links_from_sublocations(page_content, pattern)
    links = [base_url + link for link in links]
    links = set(links) - set(ran_through_urls)
    hierarchical_data = {}

    while True:
        if len(links) > 1:
            for link in links:
                page_content = fetch_page_content(driver, link)
                ran_through_urls.append(link) # remove parent
                hierarchical_data[link] = list(set(extract_links_from_sublocations(page_content, pattern)) - set(ran_through_urls))
                

            return hierarchical_data   
        elif len(links) == 0:
            return hierarchical_data            
        
        else:
            links = set(links) - set(link)

def add_sublevels(driver: webdriver.Chrome, data: dict, pattern: str) -> dict:
    base_url = "https://www.idealista.pt"
    updated_data = {}
    for parent_link, links in data.items():
        hierarchical_data = {}
        ran_through_urls = [parent_link]
        for link in links:
            full_link = base_url + link
            page_content = fetch_page_content(driver, full_link)
            sub_links = extract_links_from_sublocations(page_content, pattern)
            sub_links = [base_url + sub_link for sub_link in sub_links if (base_url + sub_link) not in ran_through_urls]
            hierarchical_data[link] = sub_links
        updated_data[parent_link] = hierarchical_data
    return updated_data


if __name__ == "__main__":
    base_url = "https://www.idealista.pt"
    initial_page = f"{base_url}/arrendar-casas/porto-distrito/mapa"
    pattern = r'^/arrendar-casas/.*/mapa$'

    driver = setup_driver()

    # fetch_page_content(driver, base_url)
    
    hierarchical_links = scrape_links_recursively(driver, initial_page, base_url, pattern)
    hierarchical_links_with_sublevels = add_sublevels(driver, hierarchical_links, pattern)
    
    driver.quit()

    with open('data.json', 'w') as f:
        json.dump(hierarchical_links_with_sublevels, f, indent=4)
