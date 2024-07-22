import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os
from extractCSV import read_csv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Read environment variables for PostgreSQL connection
postgres_host = os.getenv('postgres_host')
postgres_user = os.getenv('postgres_user')
postgres_pass = os.getenv('postgres_pass')
postgres_db = os.getenv('postgres_db')

# Function to set up PostgreSQL connection
def setup_db_connection():
    connection = psycopg2.connect(
        host=postgres_host,
        user=postgres_user,
        password=postgres_pass,
        database=postgres_db
    )
    print("Connection to PostgreSQL DB successful")
    return connection

# Function to create tables in the PostgreSQL database
def create_tables(connection):
    with connection.cursor() as cursor:
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customer_dimension (
                customer_id SERIAL PRIMARY KEY
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS location_dimension (
                location_id SERIAL PRIMARY KEY,
                location VARCHAR(255)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payment_method_dimension (
                payment_method_id SERIAL PRIMARY KEY,
                payment_method VARCHAR(255)
            );
        ''')

        cursor.execute('''
            CREATE TABLE IF NOT EXISTS fact_table (
                timestamp TIMESTAMP,
                location_id INT REFERENCES location_dimension(location_id),
                customer_id INT REFERENCES customer_dimension(customer_id),
                items_ordered TEXT,
                total_amount NUMERIC,
                payment_method_id INT REFERENCES payment_method_dimension(payment_method_id),
                PRIMARY KEY (timestamp, location_id, customer_id)
            );
        ''')

    connection.commit()
    print("Tables created successfully")

# Function to insert data into PostgreSQL
def insert_data_into_postgres(connection, data_list):
    try:
        cursor = connection.cursor()

        for row in data_list:
            # Convert timestamp to PostgreSQL-compatible format
            timestamp = datetime.strptime(row['timestamp'], '%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S')

            # Insert into customer_dimension table
            cursor.execute("""
                INSERT INTO customer_dimension DEFAULT VALUES
                RETURNING customer_id
            """)
            customer_id = cursor.fetchone()[0]

            # Check if location already exists in location_dimension table
            cursor.execute("""
                SELECT location_id FROM location_dimension WHERE location = %s
            """, (row['location'],))
            location_row = cursor.fetchone()
            if location_row:
                location_id = location_row[0]
            else:
                # Insert into location_dimension table
                cursor.execute("""
                    INSERT INTO location_dimension (location)
                    VALUES (%s)
                    RETURNING location_id
                """, (row['location'],))
                location_id = cursor.fetchone()[0]

            # Check if payment method already exists in payment_method_dimension table
            cursor.execute("""
                SELECT payment_method_id FROM payment_method_dimension WHERE payment_method = %s
            """, (row['payment_method'],))
            payment_method_row = cursor.fetchone()
            if payment_method_row:
                payment_method_id = payment_method_row[0]
            else:
                # Insert into payment_method_dimension table
                cursor.execute("""
                    INSERT INTO payment_method_dimension (payment_method)
                    VALUES (%s)
                    RETURNING payment_method_id
                """, (row['payment_method'],))
                payment_method_id = cursor.fetchone()[0]

            # Insert into fact_table
            cursor.execute("""
                INSERT INTO fact_table (timestamp, location_id, customer_id, items_ordered, total_amount, payment_method_id)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                timestamp,
                location_id,
                customer_id,
                row['items_ordered'],
                row['total_amount'],
                payment_method_id
            ))

        connection.commit()
        print("Data inserted successfully into PostgreSQL")

    except psycopg2.Error as e:
        print(f"Failed to insert data into PostgreSQL table: {e}")

    finally:
        cursor.close()

if __name__ == '__main__':
    try:
        # Set up PostgreSQL connection
        connection = setup_db_connection()

        # Create tables if they do not exist
        create_tables(connection)

        # Example paths for CSV files
        file_path1 = '/Users/saruhanarac/Desktop/Generation/creme-de-la-creme/leeds_09-05-2023_09-00-00.csv'
        file_path2 = '/Users/saruhanarac/Desktop/Generation/creme-de-la-creme/chesterfield_25-08-2021_09-00-00.csv'

        # Read data from CSV files
        data_list1 = read_csv(file_path1)
        data_list2 = read_csv(file_path2)

        # Combine data from both CSV files
        combined_data_list = data_list1 + data_list2

        # Insert combined data into PostgreSQL
        insert_data_into_postgres(connection, combined_data_list)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")