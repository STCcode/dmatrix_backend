import requests

# Your Render API URL
url = "https://dmatrix-backend.onrender.com/upload_and_save"

# JSON payload: replace entityid & category as needed
payload = {
    "entity": {
        "entityid": "ENT-0200"  # Example unique entity ID
    },
    "action": {
        "category": "mutual_fund"  # Use category like mutual_fund, aif, etf, etc.
    }
}

# Single PDF file
files = {
    "files": open("Motilal.pdf", "rb")  # Replace with your PDF path
}

try:
    response = requests.post(url, data=None, files=files, json=payload)
    print("Status Code:", response.status_code)
    
    try:
        print("Response JSON:", response.json())
    except Exception:
        print("Response Text:", response.text)

finally:
    files["files"].close()  # Always close the file
