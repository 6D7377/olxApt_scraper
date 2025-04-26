import time
import requests
from bs4 import BeautifulSoup
from database import save_to_db, log_error
import logging

# Configure logging
logging.basicConfig(
    filename='logging.log',  # Log file renamed to 'logging.log'
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Create a session for persistent connections
session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

def fetch_and_parse_html(url, max_retries):
    """Fetch and parse the HTML content of a given URL."""
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
    soup = fetch_and_parse_html(base_url, max_retries)
    if not soup:
        log_error(f"Failed to fetch or parse base URL: {base_url}")
        return []

    ad_links = []
    ad_containers = soup.find_all('div', class_='css-qfzx1y')
    for container in ad_containers:
        ad_link = container.find('a', class_='css-1tqlkj0')
        if ad_link and ad_link.get('href') and "?reason=extended_search_extended_distance" not in ad_link['href']:
            ad_links.append(f"https://www.olx.ua{ad_link['href']}")
    return ad_links

def extract_ad_details(ad_url, max_retries):
    """Extract detailed information from a single advertisement page."""
    soup = fetch_and_parse_html(ad_url, max_retries)
    if not soup:
        log_error(f"Failed to fetch or parse advertisement URL: {ad_url}")
        return None

    # Try to extract the title with fallback logic
    title = None
    title_element = soup.find('h4', class_='css-10ofhqw')
    if title_element:
        title = title_element.get_text(strip=True)
    else:
        # Fallback: Try another strategy to find the title
        title_element = soup.find('h1') or soup.find('h2')
        title = title_element.get_text(strip=True) if title_element else "No Title Found"

    return {"title": title}

def scrape_ads(base_url, table_name, total_pages, max_retries):
    """Scrape ads from multiple pages and save them to the database."""
    logging.info("Program started successfully. Beginning the scraping process.")
    total_ads = 0

    for page_number in range(1, total_pages + 1):
        page_url = f"{base_url}?page={page_number}"
        logging.info(f"Parsing page: {page_url}")
        print(f"Parsing page: {page_url}")

        # Collect links for all ads on the current page
        ad_links = extract_ad_links(page_url, max_retries)
        if not ad_links:
            logging.info(f"No ads found on page {page_number}.")
            print(f"No ads found on page {page_number}.")
            continue

        logging.info(f"Found {len(ad_links)} ads on page {page_number}.")
        print(f"Found {len(ad_links)} ads on page {page_number}.")

        for ad_link in ad_links:
            logging.info(f"Processing ad: {ad_link}")
            ad_details = extract_ad_details(ad_link, max_retries)
            if ad_details:
                # Save the extracted ad details to the database
                save_to_db(table_name, (ad_details["title"], "", "", ad_link))
                total_ads += 1
                logging.info(f"Ad saved: {ad_details['title']}")
            time.sleep(2)  # Delay to avoid being blocked

    logging.info(f"Scraping completed. Total ads scraped: {total_ads}")
    print(f"Scraping completed. Total ads scraped: {total_ads}")

# Display success message when program is run successfully
if __name__ == "__main__":
    print("Scraper module executed successfully!")