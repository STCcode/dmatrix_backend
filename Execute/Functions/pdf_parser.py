import pdfplumber
import pandas as pd
import re
import json
from PyPDF2 import PdfFileReader, PdfFileWriter


def extract_pdf_content(pdf_path_or_file, password=None):
    """
    Extract tables and metadata from PDF (Mutual Fund contract notes)
    Supports password-protected PDFs.
    """
    tables = []
    broker_name = "Unknown"

    try:
        with pdfplumber.open(pdf_path_or_file, password=password or "") as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text = page.extract_text() or ""
                if broker_name == "Unknown" and text:
                    broker_name = detect_broker_name(text)

                # Extract contract date
                contract_date = extract_date_from_text(text)

                # Stamp duty
                stamp_match = re.search(r"Stamp Duty\s+([\d.,]+)", text, re.IGNORECASE)
                stamp_duty = float(stamp_match.group(1).replace(",", "")) if stamp_match else 0.0

                # Extract tables
                page_tables = [
                    pd.DataFrame(t[1:], columns=t[0])
                    for t in page.extract_tables() if t and len(t) > 1
                ]

                # Phillip Capital fallback
                if not page_tables and "PHILLIPCAPITAL" in text.upper():
                    page_tables = parse_phillip_text_format(text)

                if not page_tables:
                    continue

                total_rows = sum(len(df) for df in page_tables)
                per_row_stamp_duty = stamp_duty / total_rows if total_rows > 0 else 0.0

                for df in page_tables:
                    df["__page__"] = page_num
                    df["__contract_date__"] = contract_date
                    df["__stamp_duty__"] = per_row_stamp_duty
                    df["__broker__"] = broker_name
                    tables.append(df)

        if not tables:
            raise ValueError("No tables extracted from PDF")

        return {"tables": tables, "broker": broker_name}

    except Exception as e:
        # Detect password error
        if "PDFPasswordIncorrect" in str(e):
            raise ValueError("PDF is password protected or wrong password provided")
        else:
            raise ValueError(f"Failed to parse PDF: {str(e)}")



#This Function is Created For PDF password

def decrypt_pdf(input_path, output_path, password):
  with open(input_path, 'rb') as input_file, \
    open(output_path, 'wb') as output_file:
    reader = PdfFileReader(input_file)
    reader.decrypt(password)

    writer = PdfFileWriter()

    for i in range(reader.getNumPages()):
      writer.addPage(reader.getPage(i))

    writer.write(output_file)



def parse_phillip_text_format(text):
    """
    Parse Phillip Capital text format when table extraction fails
    """
    lines = text.split('\n')
    transactions = []
    
    # Look for transaction lines
    for i, line in enumerate(lines):
        # Match lines that contain ISIN codes (format: INF followed by alphanumeric)
        if re.search(r'INF[A-Z0-9]{6}', line):
            # This line likely contains transaction data
            parts = line.split()
            if len(parts) >= 8:  # Ensure we have enough parts
                try:
                    # Extract data based on the format seen in your PDF
                    mutual_fund_name = parts[0] if parts else ""
                    
                    # Find scheme name (everything before ISIN)
                    isin_match = re.search(r'(INF[A-Z0-9]{6}[A-Z0-9]*)', line)
                    if isin_match:
                        isin = isin_match.group(1)
                        scheme_part = line[:isin_match.start()].strip()
                        # Extract scheme name (remove the fund code)
                        scheme_parts = scheme_part.split(' ', 1)
                        mutual_fund_scheme = scheme_parts[1] if len(scheme_parts) > 1 else scheme_part
                    else:
                        continue
                    
                    # Extract numerical values
                    numbers = re.findall(r'[\d,]+\.?\d*', line)
                    if len(numbers) >= 3:
                        purchase_units = numbers[-3].replace(',', '') if len(numbers) >= 3 else "0"
                        buy_rate = numbers[-2].replace(',', '') if len(numbers) >= 2 else "0"
                        buy_total = numbers[-1].replace(',', '') if len(numbers) >= 1 else "0"
                    else:
                        continue
                    
                    # Extract time and order number
                    time_match = re.search(r'(\d{2}:\d{2}:\d{2})', line)
                    order_time = time_match.group(1) if time_match else ""
                    
                    order_match = re.search(r'(\d{10})', line)  # 10 digit order number
                    order_no = order_match.group(1) if order_match else ""
                    
                    transactions.append({
                        'MUTUAL FUND NAME': mutual_fund_name,
                        'MUTUAL FUND SCHEME': mutual_fund_scheme,
                        'ISIN': isin,
                        'ORDER TIME': order_time,
                        'ORDER No': order_no,
                        'PURCHASE UNITS': purchase_units,
                        'BUY RATE': buy_rate,
                        'BUY TOTAL': buy_total,
                        'DATE': extract_date_from_text(text)
                    })
                except Exception as e:
                    print(f"Error parsing line: {line[:50]}... - {e}")
                    continue
    
    if transactions:
        return [pd.DataFrame(transactions)]
    return []


def extract_date_from_text(text):
    """
    Extract date from text in various formats
    """
    # Try different date formats
    date_patterns = [
        r"Date\s+(\d{2}/\d{2}/\d{4})",
        r"(\d{2}/\d{2}/\d{4})",
        r"Date:\s*(\d{2}/\d{2}/\d{4})",
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(1)
    return None


def detect_broker_name(text: str) -> str:
    brokers = {
        "motilal oswal": "Motilal Oswal Financial Services Limited",
        "zerodha": "Zerodha Broking Limited", 
        "hdfc": "HDFC Securities Limited",
        "icici": "ICICI Securities Limited",
        "phillipcapital": "Phillip Capital (India) Pvt Ltd",
        "phillip capital": "Phillip Capital (India) Pvt Ltd"
    }
    text_lower = text.lower()
    for key, fullname in brokers.items():
        if key in text_lower:
            return fullname
    return "Unknown"


def try_float(val):
    if val is None:
        return 0.0
    try:
        # Handle string values with commas
        if isinstance(val, str):
            val = val.replace(',', '').strip()
        return float(val)
    except (ValueError, TypeError):
        return 0.0


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
                "order_date": contract_date,
                "stamp_duty": per_row_stamp_duty,
                "page_number": row.get("__page__", None),
            }

            results.append({"entityTable": entity_table, "actionTable": action_table})

    return results


def build_json_phillip(tables, category, subcategory):
    """
    Build JSON for Phillip Capital PDFs - Updated to handle the actual format
    """
    results = []

    for df in tables:
        # Normalize columns
        original_columns = df.columns.tolist()
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
        
        print(f"DEBUG: Original columns: {original_columns}")
        print(f"DEBUG: Normalized columns: {df.columns.tolist()}")

        # Check for required columns with flexible matching
        required_cols = ["mutual_fund_scheme", "isin"]
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            print(f"DEBUG: Missing required columns: {missing_cols}")
            print(f"DEBUG: Available columns: {df.columns.tolist()}")
            continue

        for idx, row in df.iterrows():
            try:
                scrip_code = str(row.get("mutual_fund_name", "")).strip()
                scrip_name = str(row.get("mutual_fund_scheme", "")).strip()
                isin = str(row.get("isin", "")).strip()
                
                if not scrip_name or not isin:
                    print(f"DEBUG: Skipping row {idx} - missing scrip_name or isin")
                    continue

                # Extract numerical values
                unit = try_float(row.get("purchase_units", 0))
                nav = try_float(row.get("buy_rate", 0))
                purchase_amount = try_float(row.get("buy_total", 0))
                order_date = str(row.get("date", row.get("__contract_date__", ""))).strip()
                order_number = str(row.get("order_no", "")).strip()

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
                "cgst": try_float(row.get("cgst")),
                "cgst": try_float(row.get("sgst")),
                "igst": try_float(row.get("igst")),
                "ugst": try_float(row.get("ugst")),
                "net_amount": try_float(row.get("net_amount")),
                "order_date": order_date,
                 "stamp_duty": try_float(row.get("__stamp_duty__", 0.0)),
                "cess_value": try_float(row.get("cess_value")),
                "purchase_value ": try_float(row.get("purchase_value ")),
                "page_number": row.get("__page__", None),
            }

                results.append({"entityTable": entity_table, "actionTable": action_table})
                
            except Exception as e:
                print(f"DEBUG: Error processing row {idx}: {e}")
                continue

    return results


def process_pdf(pdf_file, category, subcategory, password=None):
    try:
        extracted = extract_pdf_content(pdf_file, password=password)
        broker = extracted["broker"]

        if broker == "Motilal Oswal Financial Services Limited":
            json_data = build_json_from_tables(extracted["tables"], category, subcategory)
        elif broker == "Phillip Capital (India) Pvt Ltd":
            json_data = build_json_phillip(extracted["tables"], category, subcategory)
        else:
            raise ValueError(f"No parser available for broker: {broker}")

        return broker, json_data

    except ValueError as ve:
        # Pass password-specific errors clearly
        raise ve
    except Exception as e:
        # Make sure exception message is never empty
        raise RuntimeError(f"Failed to parse PDF: {str(e)}") from e


if __name__ == "__main__":
    # Update this to your PDF file path
    pdf_file = "Phillips.pdf"  # Update with your actual file path
    category = "Equity"
    subcategory = "Mutual Fund"

    try:
        broker, json_data = process_pdf(pdf_file, category, subcategory)

        print(f"\nDetected Broker: {broker}")
        print(f"Number of transactions processed: {len(json_data)}")
        
        if json_data:
            print("\nSample transaction:")
            print(json.dumps(json_data[0], indent=2))
            
            # Validate data before saving
            print("\n=== VALIDATION CHECK ===")
            for i, record in enumerate(json_data[:3]):  # Check first 3 records
                entity = record.get("entityTable", {})
                action = record.get("actionTable", {})
                
                print(f"Record {i+1}:")
                print(f"  - ISIN: {entity.get('isin', 'MISSING')}")
                print(f"  - Script Name: {entity.get('scripname', 'MISSING')}")
                print(f"  - Units: {action.get('unit', 'MISSING')}")
                print(f"  - NAV: {action.get('nav', 'MISSING')}")
                print(f"  - Purchase Amount: {action.get('purchase_amount', 'MISSING')}")
                
                # Check for common issues
                if not entity.get('isin'):
                    print(f"  ⚠️ WARNING: Missing ISIN")
                if action.get('unit', 0) == 0:
                    print(f"  ⚠️ WARNING: Zero units")
                if action.get('purchase_amount', 0) == 0:
                    print(f"  ⚠️ WARNING: Zero purchase amount")
            
            # Save to file
            with open("output.json", "w") as f:
                json.dump(json_data, f, indent=4)
            print(f"\nJSON saved to output.json")
            
            # Additional check - save raw extracted data for debugging
            with open("debug_raw_data.json", "w") as f:
                debug_data = {
                    "broker": broker,
                    "total_records": len(json_data),
                    "sample_record": json_data[0] if json_data else None,
                    "all_records": json_data
                }
                json.dump(debug_data, f, indent=4)
            print("Debug data saved to debug_raw_data.json")
            
        else:
            print(" No transactions were extracted from the PDF")
            print("This might be why your API shows 'success' but inserts no data!")
            
    except Exception as e:
        print(f"Error processing PDF: {e}")
        import traceback
        traceback.print_exc()
