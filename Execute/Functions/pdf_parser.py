import pdfplumber
import pandas as pd
import re
import json


def extract_pdf_content(pdf_path_or_file):
    """
    Extract tables and metadata from PDF (Mutual Fund contract notes)
    """
    tables = []
    broker_name = "Unknown"

    with pdfplumber.open(pdf_path_or_file) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""

            if broker_name == "Unknown" and text:
                broker_name = detect_broker_name(text)

            # Dates and numbers
            contract_match = re.search(
                r"BSE MUTUAL FUND CONTRACT NOTE\s*:\s*(\d{2}/\d{2}/\d{4})", text
            )
            contract_date = contract_match.group(1) if contract_match else None

            order_match = re.search(r"Order Date\s+(\d{2}/\d{2}/\d{4})", text)
            sett_match = re.search(r"Sett No\s+(\d+)", text)
            order_date = order_match.group(1) if order_match else None
            sett_no = sett_match.group(1) if sett_match else None

            stamp_match = re.search(r"STAMPDUTY\s+([\d.,]+)", text)
            stamp_duty = float(stamp_match.group(1).replace(",", "")) if stamp_match else 0.0

            # Extract tables
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


def try_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0


def build_json_from_tables(tables, category, subcategory):
    """
    Build JSON for Motilal Oswal PDFs
    """
    results = []

    for df in tables:
        # Normalize columns
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        if "isin" not in df.columns:
            continue

        for _, row in df.iterrows():
            scrip_name = str(row.get("scrip_name") or row.get("scheme_name") or "").strip()
            if not scrip_name or scrip_name.lower() == "none":
                continue

            isin = str(row.get("isin") or "").strip()
            contract_date = row.get("__contract_date__", "Unknown")
            order_date = row.get("__order_date__", None)
            sett_no = row.get("__sett_no__", None)
            per_row_stamp_duty = row.get("__stamp_duty__", 0.0)

            entity_table = {
                "scripname": scrip_name,
                "scripcode": str(row.get("scrip_code") or ""),
                "benchmark": "0",
                "category": category,
                "subcategory": subcategory,
                "nickname": scrip_name,
                "isin": isin
            }

            action_table = {
                "scrip_code": str(row.get("scrip_code") or ""),
                "mode": str(row.get("mode") or ""),
                "order_type": str(row.get("order_type") or ""),
                "scrip_name": scrip_name,
                "isin": isin,
                "order_number": str(row.get("order_no") or ""),
                "folio_number": str(row.get("folio_no") or ""),
                "nav": try_float(row.get("nav")),
                "stt": try_float(row.get("stt")),
                "unit": try_float(row.get("unit")),
                "redeem_amount": try_float(row.get("redeem_amt") or row.get("reedem_amt")),
                "purchase_amount": try_float(row.get("purchase_amt") or row.get("purchase_amount")),
                "net_amount": try_float(row.get("net_amount")),
                "order_date": order_date,
                "sett_no": sett_no,
                "stamp_duty": per_row_stamp_duty,
                "page_number": row.get("__page__", None),
            }

            results.append({"entityTable": entity_table, "actionTable": action_table})

    return results


def build_json_phillip(tables, category, subcategory):
    """
    Build JSON for Phillip Capital PDFs
    """
    results = []

    for df in tables:
        df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

        if "mutual_fund_name" not in df.columns or "mutual_fund_scheme" not in df.columns:
            continue

        for _, row in df.iterrows():
            scrip_code = str(row.get("mutual_fund_name") or "").strip()
            scrip_name = str(row.get("mutual_fund_scheme") or "").strip()
            unit = try_float(row.get("purchase_units"))
            nav = try_float(row.get("buy_rate"))
            purchase_amount = try_float(row.get("buy_total"))
            order_date = str(row.get("date") or "").strip()
            isin = str(row.get("isin") or "").strip()

            entity_table = {
                "scripname": scrip_name,
                "scripcode": scrip_code,
                "benchmark": "0",
                "category": category,
                "subcategory": subcategory,
                "nickname": scrip_name,
                "isin": isin
            }

            action_table = {
                "scrip_code": scrip_code,
                "mode": "DEMAT",
                "order_type": "PURCHASE",
                "scrip_name": scrip_name,
                "isin": isin,
                "order_number": str(row.get("order_no") or ""),
                "folio_number": str(row.get("folio_no") or ""),
                "nav": nav,
                "stt": 0.0,
                "unit": unit,
                "redeem_amount": 0.0,
                "purchase_amount": purchase_amount,
                "net_amount": 0.0,
                "order_date": order_date,
                "sett_no": str(row.get("sett_no") or ""),
                "stamp_duty": try_float(row.get("__stamp_duty__", 0.0)),
                "page_number": row.get("__page__", None)
            }

            results.append({"entityTable": entity_table, "actionTable": action_table})

    return results


def process_pdf(pdf_file, category, subcategory):
    """
    Main function to process PDF and return JSON data
    """
    extracted = extract_pdf_content(pdf_file)
    broker = extracted["broker"]

    if broker == "Motilal Oswal Financial Services Limited":
        json_data = build_json_from_tables(extracted["tables"], category, subcategory)
    elif broker == "Phillip Capital (India) Pvt Ltd":
        json_data = build_json_phillip(extracted["tables"], category, subcategory)
    else:
        raise ValueError(f"No parser available for broker: {broker}")

    # Debug logs
    print("DEBUG: Broker ->", broker)
    print("DEBUG: Number of tables extracted ->", len(extracted["tables"]))
    for i, df in enumerate(extracted["tables"]):
        print(f"DEBUG: Table {i} columns ->", df.columns.tolist())
    print("DEBUG: JSON data length ->", len(json_data))

    return broker, json_data


if __name__ == "__main__":
    pdf_file = "Motilal.pdf"
    category = "Equity"
    subcategory = "Mutual Fund"

    broker, json_data = process_pdf(pdf_file, category, subcategory)

    print(f"Detected Broker: {broker}")
    print(json.dumps(json_data, indent=4))

    with open("output.json", "w") as f:
        json.dump(json_data, f, indent=4)
    print("JSON saved to output.json")
