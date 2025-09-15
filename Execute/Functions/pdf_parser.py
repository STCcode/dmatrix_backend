import pdfplumber
import pandas as pd
import re
from datetime import datetime

# ================= Helper Functions =================
def try_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0

def detect_broker_name(text: str) -> str:
    brokers = {
        "motilal oswal": "Motilal Oswal Financial Services Limited",
        "zerodha": "Zerodha Broking Limited",
        "hdfc": "HDFC Securities Limited",
        "icici": "ICICI Securities Limited",
        "phillip capital": "Phillip Capital (India) Pvt Ltd"
    }
    text_lower = text.lower()
    for key, fullname in brokers.items():
        if key in text_lower:
            return fullname
    return "Unknown"

# ================= PDF Extraction =================
def extract_pdf_content(pdf_path):
    tables = []
    broker_name = "Unknown"

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""

            if broker_name == "Unknown" and text:
                broker_name = detect_broker_name(text)

            # Extract tables
            page_tables = [pd.DataFrame(t[1:], columns=t[0]) 
                           for t in page.extract_tables() if t and len(t) > 1]

            if not page_tables:
                continue

            for df in page_tables:
                df["__page__"] = page_num
                tables.append(df)

    return {"tables": tables, "broker": broker_name}

# ================= Build JSON =================
def build_json_from_tables(tables, category, subcategory):
    results = []
    for df in tables:
        if "ISIN" not in df.columns and "Mutual Fund Scheme" not in df.columns:
            continue

        for _, row in df.iterrows():
            scrip_name = str(row.get("Scrip Name", row.get("Mutual Fund Scheme", ""))).strip()
            if not scrip_name:
                continue

            isin = str(row.get("ISIN", "")).strip()
            entity_table = {
                "scripname": scrip_name,
                "scripcode": str(row.get("Scrip Code", row.get("Mutual Fund Name", ""))),
                "benchmark": "0",
                "category": category,
                "subcategory": subcategory,
                "nickname": scrip_name,
                "isin": isin
            }

            action_table = {
                "scrip_code": entity_table["scripcode"],
                "mode": str(row.get("Mode", "DEMAT")),
                "order_type": str(row.get("Order Type", "PURCHASE")),
                "scrip_name": scrip_name,
                "isin": isin,
                "order_number": str(row.get("Order No", "")),
                "folio_number": str(row.get("Folio No", "")),
                "nav": try_float(row.get("NAV", row.get("Buy Rate"))),
                "stt": try_float(row.get("STT", 0)),
                "unit": try_float(row.get("Unit", row.get("Purchase Units"))),
                "redeem_amount": try_float(row.get("Reedem Amt", 0)),
                "purchase_amount": try_float(row.get("Purchase Amt", row.get("Buy Total", 0))),
                "net_amount": try_float(row.get("Net Amount", 0)),
                "order_date": str(row.get("__order_date__", row.get("Date", ""))),
                "sett_no": str(row.get("__sett_no__", "")),
                "stamp_duty": try_float(row.get("__stamp_duty__", 0.0)),
                "page_number": row.get("__page__", None)
            }

            results.append({"entityTable": entity_table, "actionTable": action_table})

    return results

# ================= Main API Function =================
def process_pdf(pdf_file, category, subcategory):
    extracted = extract_pdf_content(pdf_file)
    broker = extracted["broker"]
    json_data = build_json_from_tables(extracted["tables"], category, subcategory)
    return broker, json_data
