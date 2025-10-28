# db.py

import os
import psycopg2
import urllib.parse as urlparse
import yaml  # only needed if you want to support db.yaml fallback

def get_db_connection():
    if 'DATABASE_URL' in os.environ:
        print("Connecting via Render DATABASE_URL...")
        result = urlparse.urlparse(os.environ['DATABASE_URL'])
        return psycopg2.connect(
            host=result.hostname,
            user=result.username,
            password=result.password or '',
            dbname=result.path[1:],
            port=result.port,
            sslmode='require'
        )
    else:
        print("Connecting via db.yaml (local dev)...")
        db = yaml.safe_load(open('db.yaml'))
        return psycopg2.connect(
            host=db['localhost'],
            user=db['postgres'],
            password=db['Admin@123'],
            dbname=db['db_dmatrix']
        )
