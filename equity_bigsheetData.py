import pandas as pd
from datetime import datetime
import os
from sqlalchemy import create_engine
from db import get_db_connection  # your db.py

downloads_path = r"D:/equity_bigsheetData"

def load_file(filename):
    file_path = os.path.join(downloads_path, filename)
    
    # Load CSV or Excel
    if filename.lower().endswith(".csv"):
        df = pd.read_csv(file_path)
    elif filename.lower().endswith((".xlsx", ".xls")):
        df = pd.read_excel(file_path)
    else:
        raise ValueError(f"Unsupported file type: {filename}")
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    
    # Drop Unnamed / empty columns
    unnamed_cols = df.columns[df.columns.str.contains("^unnamed", case=False)]
    if len(unnamed_cols) > 0:
        print(f"⚠️ Dropped {len(unnamed_cols)} unnamed column(s) from {filename}")
        df = df.loc[:, ~df.columns.str.contains("^unnamed", case=False)]
    
    print(f"📄 Cleaned Columns in {filename} → {list(df.columns)}")
    return df

def find_isin_column(df):
    candidates = ['isin_code', 'isin_number', 'isin', 'isin_no']
    for c in candidates:
        if c in df.columns:
            # Standardize ISIN values
            df[c] = df[c].astype(str).str.strip().str.upper()
            return c
    raise KeyError("No ISIN column found in dataframe")

def get_sqlalchemy_engine():
    raw_conn = get_db_connection()
    engine = create_engine('postgresql+psycopg2://', creator=lambda: raw_conn)
    return engine

def main():
    # Load files
    bse = load_file("BSE_T1.csv")
    nse = load_file("NSE_EQUITY_L.csv")
    sme = load_file("SME_EQUITY_L.csv")
    tag_sheet = load_file("bigsheet_with_tags.xlsx")

    # Detect ISIN columns
    bse_isin = find_isin_column(bse)
    nse_isin = find_isin_column(nse)
    sme_isin = find_isin_column(sme)
    tag_isin = find_isin_column(tag_sheet)

    # Filter BSE → only Active
    if "status" in bse.columns:
        bse = bse[bse["status"].str.strip().str.lower() == "active"]

    # Add source column
    bse["source"] = "BSE_T1"
    nse["source"] = "NSE_Equity_L"
    sme["source"] = "SME_Equity_L"

    # --- Combine NSE + SME into one dataset ---
    nse_sme = pd.concat([nse, sme], ignore_index=True)

    # Merge BSE with combined NSE+SME (on ISIN)
    merged = pd.merge(
        bse,
        nse_sme,
        how="outer",
        left_on=bse_isin,
        right_on=nse_isin,
        suffixes=("_bse", "_nse")
    )

    # --- Unify duplicate columns ---
    dup_cols = [col for col in merged.columns if col.endswith("_bse") or col.endswith("_nse")]
    for col in set(c.rsplit("_", 1)[0] for c in dup_cols):
        col_bse = f"{col}_bse"
        col_nse = f"{col}_nse"
        if col_bse in merged.columns and col_nse in merged.columns:
            # Prefer BSE value, else NSE
            merged[col] = merged[col_bse].combine_first(merged[col_nse])
            merged = merged.drop(columns=[col_bse, col_nse])
        elif col_bse in merged.columns:
            merged = merged.rename(columns={col_bse: col})
        elif col_nse in merged.columns:
            merged = merged.rename(columns={col_nse: col})

    # --- Handle ISIN cleanly (final single column) ---
    merged["isin"] = merged[bse_isin].combine_first(merged[nse_isin])
    merged = merged.drop(columns=[c for c in [bse_isin, nse_isin] if c in merged.columns])

    # --- Merge Tags ---
    if "tag" in tag_sheet.columns:
        merged = pd.merge(
            merged,
            tag_sheet[[tag_isin, "tag"]],
            how="left",
            left_on="isin",
            right_on=tag_isin
        ).drop(columns=[tag_isin])

         # 🔹 Normalize tag values (case-insensitive)
        merged["tag"] = merged["tag"].str.lower().str.strip()
        tag_map = {
            "lcap": "large cap",
            "scap": "small cap",
            "mcap": "mid cap"
        }
        merged["tag"] = merged["tag"].map(tag_map).fillna("small cap")

    else:
        print("⚠️ 'tag' column not found in bigsheet_with_tags.xlsx")

    # --- Merge sources into unique list ---
    merged["source"] = merged["source"].fillna("").astype(str)
    merged["source"] = merged["source"].str.split(",")
    merged["source"] = merged["source"].apply(lambda x: list(set([s.strip() for s in x if s.strip()])))
    merged["source"] = merged["source"].apply(lambda x: ",".join(sorted(x)) if x else "NA")

    # Fill NA where data missing
    merged = merged.fillna("NA")

    # Final file name
    today_str = datetime.now().strftime("%Y-%m-%d")
    output_file = os.path.join(downloads_path, f"equity_bigsheet_data_{today_str}.xlsx")
    merged.to_excel(output_file, index=False)
    print(f"✅ Final file saved locally at: {output_file}")

    # Save to PostgreSQL
    engine = get_sqlalchemy_engine()
    merged.to_sql(
        'equity_bigsheet_data',
        con=engine,
        if_exists='replace',
        index=False
    )
    print("✅ Data saved to PostgreSQL table 'equity_bigsheet_data'")

if __name__ == "__main__":
    main()
