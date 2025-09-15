import pdfplumber
import pandas as pd
import re
import json

def extract_pdf_content(pdf_path_or_file):
    tables = []
    broker_name = "Unknown"

    with pdfplumber.open(pdf_path_or_file) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""

            if broker_name == "Unknown" and text:
                broker_name = detect_broker_name(text)

            # Dates and numbers
            contract_match = re.search(r"BSE MUTUAL FUND CONTRACT NOTE\s*:\s*(\d{2}/\d{2}/\d{4})", text)
            contract_date = contract_match.group(1) if contract_match else None

            order_match = re.search(r"Order Date\s+(\d{2}/\d{2}/\d{4})", text)
            sett_match = re.search(r"Sett No\s+(\d+)", text)
            order_date = order_match.group(1) if order_match else None
            sett_no = sett_match.group(1) if sett_match else None

            stamp_match = re.search(r"STAMPDUTY\s+([\d.,]+)", text)
            stamp_duty = float(stamp_match.group(1).replace(",", "")) if stamp_match else 0.0

            page_tables = [
                pd.DataFrame(t[1:], columns=t[0])
                for t in page.extract_tables() if t and len(t) > 1
            ]
            if not page_tables:
                continue

            total_rows = sum(len(df) for df in page_tables)
            per_row_stamp_duty = stamp_duty / total_rows if total_rows > 0 else 0.0

            for df in page_tables:
                df["__page__"] = page_num
                df["__contract_date__"] = contract_date
                df["__order_date__"] = order_date
                df["__sett_no__"] = sett_no
                df["__stamp_duty__"] = per_row_stamp_duty
                df["__broker__"] = broker_name
                tables.append(df)

    return {"tables": tables, "broker": broker_name}


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


def build_json_from_tables(tables, category, subcategory):
    # (same as before, no database logic here, just JSON building)
    ...


def build_json_phillip(tables, category, subcategory):
    # (same as before)
    ...


def process_pdf(pdf_file, category, subcategory):
    extracted = extract_pdf_content(pdf_file)
    broker = extracted["broker"]

    if broker == "Motilal Oswal Financial Services Limited":
        json_data = build_json_from_tables(extracted["tables"], category, subcategory)
    elif broker == "Phillip Capital (India) Pvt Ltd":
        json_data = build_json_phillip(extracted["tables"], category, subcategory)
    else:
        raise ValueError(f"No parser available for broker: {broker}")

    return broker, json_data


def try_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0
