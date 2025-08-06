import psycopg2
from psycopg2 import OperationalError
# import os
# from dotenv import load_dotenv


# # Load .env file (only in development)
# load_dotenv()

# # Function to create a database connection
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

# # Function to query data from a table
# def fetch_data():
#     connection = create_connection()
#     if connection:
#         try:
#             cursor = connection.cursor()
#             cursor.execute("SELECT * FROM your_table")  # Replace with your actual table name
            
#             # Fetch all results from the executed query
#             result = cursor.fetchall()
#             for row in result:
#                 print(row)  # Print each row from the table
                
#             cursor.close()
#         except OperationalError as e:
#             print(f"Error executing query: {e}")
#         finally:
#             connection.close()

# # Run the function to fetch data
# if __name__ == "__main__":
#     fetch_data()


def create_connection():
    try:
        connection = psycopg2.connect(
            dbname="dmatrix_db",
            user="dmatrix_user",
            password="your_password",
            host="dpg-d29gq5vgi27c73cipjqg-a",
            port="5432"
        )
        print("Connection to PostgreSQL successful")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred")
        return None

def fetch_data():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM your_table;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
        cursor.close()
        connection.close()
    else:
        print("Failed to connect to DB")

if __name__ == "__main__":
    fetch_data()