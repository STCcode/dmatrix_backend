import pandas as pd
from datetime import datetime
import os
from db import get_db_connection
from sqlalchemy import create_engine, event

downloads_path = r"D:\bigsheet"

def load_file(filename):
    file_path = os.path.join(downloads_path, filename)
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(file_path)
    elif filename.lower().endswith(".xlsx"):
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {filename}")
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    print(f"📄 Columns in {filename} → {list(df.columns)}")
    return df

def find_isin_column(df):
    candidates = ['isin_code', 'isin_number', 'isin', 'isin_no']
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError("No ISIN column found in dataframe")

def get_sqlalchemy_engine():
    # Get raw psycopg2 connection
    raw_conn = get_db_connection()

    # Create SQLAlchemy engine from raw connection using creator function
    engine = create_engine('postgresql+psycopg2://', creator=lambda: raw_conn)

    # Prevent SQLAlchemy from closing the raw connection after each use
    @event.listens_for(engine, "engine_connect")
    def set_raw_connection(connection, branch):
        if branch:
            return
        connection.connection = raw_conn

    return engine

def main():
    lcap = load_file("lcap.xlsx")
    mcap = load_file("mcap.xlsx")
    scap = load_file("scap.xlsx")
    bigsheet = load_file("bigsheet.xlsx")

    lcap_isin_col = find_isin_column(lcap)
    mcap_isin_col = find_isin_column(mcap)
    scap_isin_col = find_isin_column(scap)
    bigsheet_isin_col = find_isin_column(bigsheet)

    tag_map = {}
    for df, tag, isin_col in [(lcap, "lcap", lcap_isin_col), (mcap, "mcap", mcap_isin_col), (scap, "scap", scap_isin_col)]:
        for isin in df[isin_col]:
            tag_map[str(isin).strip().upper()] = tag

    bigsheet["tag"] = bigsheet[bigsheet_isin_col].apply(lambda x: tag_map.get(str(x).strip().upper(), "other"))

    today_str = datetime.now().strftime("%Y-%m-%d")
    output_file = os.path.join(downloads_path, f"bigsheet_with_tags_{today_str}.xlsx")
    bigsheet.to_excel(output_file, index=False)
    print(f"✅ Final file saved locally at: {output_file}")

    # Save to Postgres
    engine = get_sqlalchemy_engine()

    bigsheet.to_sql(
        'bigsheet_data',
        con=engine,
        if_exists='replace',
        index=False
    )
    print("✅ Data saved to PostgreSQL table 'bigsheet_data'")

if __name__ == "__main__":
    main()
