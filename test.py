import psycopg2
from psycopg2 import OperationalError 

try:
    # Establish a connection to PSQL database
    connection = psycopg2.connector.connect(
        host='127.0.0.1',
        database='db_mutualfund',  # Replace with your database name
        user='postgres',      # Replace with your PSQL username
        password='Admin@123'   # Replace with your PSQL password
    )
    
    # Check if connection is successful
    if connection.is_connected():
        print("Successfully connected to the PSQL database")
        cursor = connection.cursor()  # Create a cursor object
        # Now you can execute queries using the cursor
    else:
        print("Connection to PSQL database failed")
except OperationalError as e:
    print(f"Error: {e}")
    connection = None
