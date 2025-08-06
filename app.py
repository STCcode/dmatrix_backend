import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv


# Load .env file (only in development)
load_dotenv()


def create_connection():
    try:
        # Connect using DATABASE_URL environment variable
        DATABASE_URL = os.getenv("DATABASE_URL")

        if not DATABASE_URL:
            print("Error: DATABASE_URL environment variable is not set.")
            return None

        connection = psycopg2.connect(DATABASE_URL)
        print("Connected to the database")
        return connection

    except OperationalError as e:
        print(f"Error: {e}")
        return None

# Function to create a database connection
# def create_connection():
#     try:
#         # # Connect to PSQL database
#         # connection = psycopg2.connect(
#         #     host='localhost',          # PSQL host
#         #     user='postgres',      # PSQL username
#         #     password='Admin@123',  # PSQL password
#         #     database='db_dmatrix'  # PSQL database name
#         # )

#           # Connect using DATABASE_URL from .env or Render
#         connection = psycopg2.connect(os.getenv("DATABASE_URL"))
        
#         if connection.is_connected():
#             print("Connected to the database")
#             return connection
#         else:
#             print("Failed to connect to the database")
#             return None
#     except OperationalError as e:
#         print(f"Error: {e}")
#         return None

# Function to query data from a table
def fetch_data():
    connection = create_connection()
    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT * FROM tbl_user")  # Replace with your actual table name
            
            # Fetch all results from the executed query
            result = cursor.fetchall()
            for row in result:
                print(row)  # Print each row from the table
                
            cursor.close()
        except OperationalError as e:
            print(f"Error executing query: {e}")
        finally:
            connection.close()

# Run the function to fetch data
if __name__ == "__main__":
    fetch_data()
