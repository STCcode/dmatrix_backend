from flask import Flask, jsonify
import requests
import pandas as pd
from io import StringIO
import psycopg2
from psycopg2.extras import execute_batch
from sqlalchemy import create_engine
import schedule
# import threading
import time
from datetime import datetime

# -----------------------------
# Step 0: Flask App
# -----------------------------
app = Flask(__name__)

# Scheduler control
scheduler_thread = None
stop_scheduler = False

# -----------------------------
# Step 1: NAV Functions
# -----------------------------
def download_amfi_nav():
    url = "https://www.amfiindia.com/spages/NAVAll.txt"

    response = requests.get(url, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to download NAV file: {response.status_code}")
    response.encoding = "utf-8"

    lines = response.text.splitlines()
    header_index = None
    for i, line in enumerate(lines):
        if line.startswith("Scheme Code;"):
            header_index = i
            break
    if header_index is None:
        raise ValueError("Could not find header row in NAV file!")

    valid_text = "\n".join(lines[header_index:])
    df = pd.read_csv(StringIO(valid_text), sep=";", dtype=str)

    df = df.dropna(subset=["ISIN Div Payout/ ISIN Growth", "Net Asset Value"])
    df["Net Asset Value"] = df["Net Asset Value"].str.replace(",", "", regex=False)
    df["Net Asset Value"] = pd.to_numeric(df["Net Asset Value"], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")

    return df

def save_to_postgres(df):
    conn = psycopg2.connect(
        "postgresql://dmarix_backend_7acm_user:ApRUMAwAMlKyE4bZSSIzupLyHI6k8TKl@dpg-d2tba0uuk2gs73cn7fdg-a.oregon-postgres.render.com/dmarix_backend_7acm"
    )
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tbl_mutual_fund_nav (
            id SERIAL PRIMARY KEY,
            scheme_code VARCHAR(20),
            isin VARCHAR(20),
            scheme_name TEXT,
            nav NUMERIC,
            nav_date DATE,
            UNIQUE (scheme_code, nav_date)
        );
        """
    )

    records = []
    for _, r in df.iterrows():
        records.append(
            (
                r["Scheme Code"],
                r["ISIN Div Payout/ ISIN Growth"],
                r.get("Scheme Name", ""),
                r["Net Asset Value"],
                r["Date"].date() if pd.notna(r["Date"]) else None,
            )
        )

    sql = """
        INSERT INTO tbl_mutual_fund_nav (scheme_code, isin, scheme_name, nav, nav_date)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (scheme_code, nav_date)
        DO UPDATE SET nav = EXCLUDED.nav;
    """
    execute_batch(cur, sql, records, page_size=1000)
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted/updated {len(records)} rows into tbl_mutual_fund_nav")

def fetch_latest_navs():
    engine = create_engine(
        "postgresql+psycopg2://dmarix_backend_7acm_user:ApRUMAwAMlKyE4bZSSIzupLyHI6k8TKl@dpg-d2tba0uuk2gs73cn7fdg-a.oregon-postgres.render.com/dmarix_backend_7acm"
    )

    query = """
    SELECT scheme_code, isin, scheme_name, nav, nav_date
    FROM tbl_mutual_fund_nav
    WHERE nav_date = (SELECT MAX(nav_date) FROM tbl_mutual_fund_nav)
    ORDER BY scheme_code;
    """

    df = pd.read_sql(query, engine)
    print(f" Latest NAVs for {len(df)} schemes:")
    print(df.to_string(index=False))

# -----------------------------
# Step 2: Scheduler Task
# -----------------------------
def daily_nav_update():
    print(f" Running NAV update: {datetime.now()}")
    try:
        df = download_amfi_nav()
        print(" Downloaded rows:", len(df))
        save_to_postgres(df)
        fetch_latest_navs()
        print(" NAV update completed successfully.\n")
    except Exception as e:
        print(f" Error during NAV update: {e}")

def run_scheduler():
    global stop_scheduler
    while not stop_scheduler:
        schedule.run_pending()
        time.sleep(1)

# -----------------------------
# Step 3: Flask Routes
# -----------------------------
# @app.route("/start", methods=["GET"])
# def start_nav_scheduler():
#     global scheduler_thread, stop_scheduler
#     stop_scheduler = False
#     schedule.clear()
#     schedule.every(1).minutes.do(daily_nav_update)  # For testing, adjust interval
#     if scheduler_thread is None or not scheduler_thread.is_alive():
#         scheduler_thread = threading.Thread(target=run_scheduler)
#         scheduler_thread.start()
#     return jsonify({"status": "Scheduler started"})

# @app.route("/stop", methods=["GET"])
# def stop_nav_scheduler():
#     global stop_scheduler
#     stop_scheduler = True
#     return jsonify({"status": "Scheduler stopped"})

# -----------------------------
# Step 4: Run Flask
# -----------------------------
# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)
