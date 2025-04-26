import mysql.connector
import logging
import os
from decouple import config
import re

# Configure logging
logging.basicConfig(
    filename='logging.log',  # Log file renamed to 'logging.log'
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='a'
)

# Load database configuration from environment variables
db_connection = mysql.connector.connect(
    host=config("DB_HOST", default="localhost"),
    user=config("DB_USER", default="root"),
    password=config("DB_PASSWORD", default=""),
    database=config("DB_NAME", default="olx"),
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci'
)
cursor = db_connection.cursor()

def sanitize_table_name(city_name):
    """Sanitize the city name to create a valid SQL table name."""
    sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '', city_name.replace('-', '_').replace(' ', '_'))
    if not sanitized_name:
        raise ValueError("City name results in an invalid table name.")
    return f"ads_{sanitized_name}"

def create_city_table(city_name):
    """Create a table for a specific city."""
    try:
        table_name = sanitize_table_name(city_name)
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(255),
            price VARCHAR(50),
            area VARCHAR(50),
            link TEXT,
            UNIQUE (link(255))  -- Specify index length for 'link'
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
        """)
        db_connection.commit()
        return table_name
    except mysql.connector.Error as e:
        log_error(f"Database table creation error for {city_name}: {e}")
        return None

def save_to_db(table_name, data):
    """Save data to the database."""
    try:
        cursor.execute(f"""
        INSERT IGNORE INTO {table_name} (title, price, area, link)
        VALUES (%s, %s, %s, %s)
        """, data)
        db_connection.commit()
        logging.info(f"Record successfully inserted into {table_name}: {data}")
    except mysql.connector.Error as e:
        log_error(f"Database insertion error for record {data}: {e}")

def close_connection():
    """Close database connection."""
    cursor.close()
    db_connection.close()

def log_error(message):
    """Log errors."""
    logging.error(message)

# Display success message when program is run successfully
if __name__ == "__main__":
    print("Database module loaded successfully!")