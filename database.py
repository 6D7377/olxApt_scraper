import mysql.connector
import logging
import json

# Configure logging
logging.basicConfig(filename='scraper.log', level=logging.ERROR)

# Load database configuration
with open("config.json", "r") as config_file:
    config = json.load(config_file)

db_connection = mysql.connector.connect(
    host=config["database"]["host"],
    user=config["database"]["user"],
    password=config["database"]["password"],
    database=config["database"]["database_name"],
    charset='utf8mb4',
    collation='utf8mb4_unicode_ci'
)
cursor = db_connection.cursor()

def create_city_table(city_name):
    """Create a table for a specific city."""
    table_name = f"ads_{city_name.replace(' ', '_')}"
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

def save_to_db(table_name, data):
    """Save data to the database."""
    try:
        cursor.execute(f"""
        INSERT IGNORE INTO {table_name} (title, price, area, link)
        VALUES (%s, %s, %s, %s)
        """, data)
        db_connection.commit()
    except mysql.connector.Error as e:
        log_error(f"Database insertion error: {e}")

def close_connection():
    """Close database connection."""
    cursor.close()
    db_connection.close()

def log_error(message):
    """Log errors."""
    logging.error(message)
    print(message)