from flask import Flask
import psycopg2
import os
from dotenv import load_dotenv
from psycopg2 import OperationalError

load_dotenv()

app = Flask(__name__)

def create_connection():
    try:
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

@app.route("/")
def home():
    return "Hello, Flask is running with Gunicorn!"

@app.route("/users")
def users():
    connection = create_connection()
    if not connection:
        return "DB connection failed", 500

    cursor = connection.cursor()
    cursor.execute("SELECT * FROM tbl_users")
    rows = cursor.fetchall()
    cursor.close()
    connection.close()

    return {"users": rows}

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)