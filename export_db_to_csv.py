import os
import csv
import mysql.connector
import logging
import json

# Configure logging
logging.basicConfig(filename='scraper.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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

def export_table_to_csv(table_name, output_file):
    """Export a database table to a CSV file."""
    try:
        # Query data from the table
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()

        # Get column names from the cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Ensure the exports directory exists
        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # Write data to a CSV file
        with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
            csv_writer = csv.writer(csv_file)
            csv_writer.writerow(column_names)  # Write header
            csv_writer.writerows(rows)        # Write data rows

        logging.info(f"Data successfully exported from {table_name} to {output_file}")
        print(f"Data successfully exported to {output_file}")
    except mysql.connector.Error as e:
        log_error(f"Error exporting table {table_name} to CSV: {e}")

def log_error(message):
    """Log errors."""
    logging.error(message)
    print(message)

def close_connection():
    """Close database connection."""
    cursor.close()
    db_connection.close()

# Example usage
if __name__ == "__main__":
    # Prompt the user to input the city name
    city_name = input("Enter the city name: ").strip()
    table_name = f"ads_{city_name.replace(' ', '_')}"  # Generate table name dynamically

    # Define the output file path in the 'exports' folder
    output_file = os.path.join("exports", f"{table_name}.csv")

    # Export the table to CSV
    export_table_to_csv(table_name, output_file)
    close_connection()