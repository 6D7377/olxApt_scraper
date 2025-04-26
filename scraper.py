import time
import requests
from bs4 import BeautifulSoup
from database import save_to_db, log_error

# Create a session for persistent connections
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

def get_page_soup(url, max_retries):
    """Fetch and parse the HTML content of a given page."""
    retries = 0
    while retries < max_retries:
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.text, 'lxml')
        except requests.exceptions.RequestException as e:
            retries += 1
            time.sleep(3)
            log_error(f"Error fetching {url} (attempt {retries}/{max_retries}): {e}")
    return None

def extract_ad_links(base_url, max_retries):
    """Extract all advertisement links from the main page."""
    soup = get_page_soup(base_url, max_retries)
    if not soup:
        log_error(f"Failed to fetch or parse base URL: {base_url}")
        return []

    ad_links = []
    ad_containers = soup.find_all('div', class_='css-qfzx1y')
    for container in ad_containers:
        ad_link = container.find('a', class_='css-1tqlkj0')
        if ad_link and ad_link.get('href'):
            ad_links.append(f"https://www.olx.ua{ad_link['href']}")
    return ad_links

def extract_ad_details(ad_url, max_retries):
    """Extract detailed information from a single advertisement page."""
    soup = get_page_soup(ad_url, max_retries)
    if not soup:
        log_error(f"Failed to fetch or parse advertisement URL: {ad_url}")
        return None

    # Extract the title
    title_element = soup.find('h4', class_='css-10ofhqw')
    title = title_element.get_text(strip=True) if title_element else "No Title Found"

    return {"title": title}

def scrape_ads(base_url, table_name, total_pages, max_retries):
    """Scrape ads from multiple pages and save them to the database."""
    total_ads = 0
    for page_number in range(1, total_pages + 1):
        page_url = f"{base_url}?page={page_number}"
        print(f"Parsing page: {page_url}")

        # Use get_page_soup to fetch and parse HTML
        soup = get_page_soup(page_url, max_retries)
        if soup:
            ads = soup.find_all('a', href=True)
            ads_count = 0

            for ad in ads:
                # Skip ads with '?reason=extended_search_extended_distance' in the URL
                link = ad.get('href')
                if not link or "?reason=extended_search_extended_distance" in link:
                    continue

                # Extract ad details
                title = ad.find('h4')
                price = ad.find_next('p', class_='css-uj7mm0')
                area = ad.find_next('span', class_='css-6as4g5')

                if title and price and area and link:
                    full_link = f"https://www.olx.ua{link}"
                    save_to_db(table_name, (title.get_text(strip=True), price.get_text(strip=True), area.get_text(strip=True), full_link))
                    ads_count += 1
                    total_ads += 1

            print(f"Found {ads_count} ads on page {page_number}.\n")
        else:
            print(f"Failed to fetch data from {page_url}.")
        
        # Delay to avoid being blocked
        time.sleep(2)

    print(f"Total ads scraped: {total_ads}")