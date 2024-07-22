from psycopg2 import sql
import os

#CREATING TABLES
def create_tables(connection):
    with connection.cursor() as cursor:
        # Create locations table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS locations (
                location_id INT IDENTITY(1,1) PRIMARY KEY,
                location VARCHAR(255) NOT NULL
            );
        ''')

        # Create payment_methods table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS payment_methods (
                payment_method_id INT IDENTITY(1,1) PRIMARY KEY,
                payment_method VARCHAR(255) NOT NULL
            );
        ''')

        # Create transactions table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id INT IDENTITY(1,1) PRIMARY KEY,
                transaction_date DATE NOT NULL,
                transaction_time TIME NOT NULL,
                location_id INT NOT NULL REFERENCES locations(location_id),
                order_total DECIMAL(10, 2) NOT NULL,
                payment_method_id INT NOT NULL REFERENCES payment_methods(payment_method_id),
                item_count INT NOT NULL
            );
        ''')

        # Create items table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS items (
                item_id INT IDENTITY(1,1) PRIMARY KEY,
                transaction_id INT NOT NULL REFERENCES transactions(transaction_id),
                item_name VARCHAR(100) NOT NULL,
                price DECIMAL(10, 2) NOT NULL
            );
        ''')
        

    connection.commit()
    print("Tables created successfully")





#INSERTING DATA
def insert_location_data(connection, data):
    with connection.cursor() as cursor:
        for row in data:
            location = row["location"]
            cursor.execute('''
                INSERT INTO locations (location)
                VALUES (%s)
            ''', (location,))
    connection.commit()
    print("Data inserted into locations successfully")

    


def insert_payment_method_data(connection, data):
    with connection.cursor() as cursor:
        for row in data:
            payment_method = row["payment_method"]
            cursor.execute('''
                INSERT INTO payment_methods (payment_method)
                VALUES (%s)
            ''', (payment_method,))
    connection.commit()
    print("Data inserted into payment_methods successfully")


def insert_transaction_data(connection, data):
    with connection.cursor() as cursor:
        for row in data:
            transaction_date = row["date"]
            transaction_time = row["time"]
            location = row["location"]
            order_total = row["order_total"]
            payment_method = row["payment_method"]
            item_count = row["item_count"]

            # Get the location_id
            cursor.execute('''
                SELECT location_id FROM locations WHERE location = %s
            ''', (location,))
            location_id = cursor.fetchone()[0]

            # Get the payment_method_id
            cursor.execute('''
                SELECT payment_method_id FROM payment_methods WHERE payment_method = %s
            ''', (payment_method,))
            payment_method_id = cursor.fetchone()[0]

            # Insert the transaction
            cursor.execute('''
                INSERT INTO transactions (transaction_date, transaction_time, location_id, order_total, payment_method_id, item_count)
                VALUES (%s, %s, %s, %s, %s, %s)
            ''', (transaction_date, transaction_time, location_id, order_total, payment_method_id, item_count))
    connection.commit()
    print("Data inserted into transactions successfully")




def insert_item_data(connection, data):
    with connection.cursor() as cursor:
        for row in data:
            transaction_date = row["date"]
            transaction_time = row["time"]
            location = row["location"]

            cursor.execute('''
                SELECT transaction_id FROM transactions
                JOIN locations ON transactions.location_id = locations.location_id
                WHERE transaction_date = %s AND transaction_time = %s AND location = %s
            ''', (transaction_date, transaction_time, location))
            transaction_id = cursor.fetchone()[0]

            for item in row["items"]:
                item_name = item["name"]
                price = item["price"]
                cursor.execute('''
                    INSERT INTO items (transaction_id, item_name, price)
                    VALUES (%s, %s, %s)
                ''', (transaction_id, item_name, price))
    connection.commit()
    print("Data inserted into items successfully")
    
    
    
