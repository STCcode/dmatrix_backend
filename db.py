# db.py

import os
import psycopg2
import urllib.parse as urlparse
import yaml  # only needed if you want to support db.yaml fallback

# def get_db_connection():
#     if 'DATABASE_URL' in os.environ:
#         print("Connecting via Render DATABASE_URL...")
#         result = urlparse.urlparse(os.environ['DATABASE_URL'])
#         return psycopg2.connect(
#             host=result.hostname,
#             user=result.username,
#             password=result.password or '',
#             dbname=result.path[1:],
#             port=result.port,
#             sslmode='require'
#         )
#     else:
#         print("Connecting via db.yaml (local dev)...")
#         db = yaml.safe_load(open('db.yaml'))
#         return psycopg2.connect(
#             host=db['postgres_host'],
#             user=db['postgres_user'],
#             password=db['postgres_password'],
#             dbname=db['postgres_db']
#         )




# db connection for data can be insert in local and server

def get_db_connection():
    connections = []

    # Render connection
    try:
        if 'DATABASE_URL' in os.environ:
            print("Connecting via Render DATABASE_URL...")
            result = urlparse.urlparse(os.environ['DATABASE_URL'])
            render_conn = psycopg2.connect(
                host=result.hostname,
                user=result.username,
                password=result.password or '',
                dbname=result.path[1:],
                port=result.port,
                sslmode='require'
            )
            connections.append(render_conn)
    except Exception as e:
        print("⚠️ Could not connect to Render DB:", e)

    # Local connection
    try:
        print("Connecting via db.yaml (local dev)...")
        db = yaml.safe_load(open('db.yaml'))
        local_conn = psycopg2.connect(
            host=db['postgres_host'],
            user=db['postgres_user'],
            password=db['postgres_password'],
            dbname=db['postgres_db']
        )
        connections.append(local_conn)
    except Exception as e:
        print("⚠️ Could not connect to Local DB:", e)

    return connections