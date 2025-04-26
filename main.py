import json
import re
import requests
from database import create_city_table, close_connection
from scraper import scrape_ads

# Load configuration
with open("config.json", "r") as config_file:
    config = json.load(config_file)

def validate_city_input(city):
    """Validate and clean the city input."""
    if not city.strip():
        raise ValueError("City name cannot be empty.")
    
    # Basic validation for city format (e.g., only letters, spaces, and dashes allowed)
    if not re.match(r"^[a-zA-Zа-яА-Я\s-]+$", city):
        raise ValueError("City name contains invalid characters.")
    
    return city.lower().replace(" ", "-")

def is_city_available(city):
    """Check if the city page exists by making an HTTP request."""
    base_url = f"https://www.olx.ua/uk/{city}/q-%D0%BE%D1%80%D0%B5%D0%BD%D0%B4%D0%B0-%D0%B6%D0%B8%D1%82%D0%BB%D0%B0/"
    try:
        response = requests.head(base_url, timeout=10)
        if response.status_code == 404:
            print(f"The city '{city}' is not available on the platform.")
            return False
        return True
    except requests.exceptions.RequestException as e:
        print(f"Error checking city availability: {e}")
        return False

def main():
    try:
        # User input for city
        city = input("Enter city for search: ")
        city = validate_city_input(city)

        # Check if the city is available
        if not is_city_available(city):
            return  # Exit if the city is not available

        # Create table for the city
        table_name = create_city_table(city)

        # Base URL for scraping
        base_url = f"https://www.olx.ua/uk/{city}/q-%D0%BE%D1%80%D0%B5%D0%BD%D0%B4%D0%B0-%D0%B6%D0%B8%D1%82%D0%BB%D0%B0/"
        
        # Scrape ads and save to database
        scrape_ads(base_url, table_name, config["number_of_pages"], config["max_retries"])
    
    except ValueError as e:
        print(f"Input Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    finally:
        # Close database connection
        close_connection()

if __name__ == "__main__":
    main()