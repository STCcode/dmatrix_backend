import pandas as pd
from datetime import datetime
import os

# Path to your Downloads folder (change if needed)
# downloads_path = r"C:\Users\YourUserName\Downloads"
downloads_path= r"D:\bigsheet"

# Load sheets
lcap = pd.read_excel(os.path.join(downloads_path, "lcap.xlsx"))
mcap = pd.read_excel(os.path.join(downloads_path, "mcap.csv"))
scap = pd.read_excel(os.path.join(downloads_path, "scap.csv"))
bigsheet = pd.read_excel(os.path.join(downloads_path, "bigsheet.csv"))

# Create a mapping of isin_code → tag from all three
tag_map = {}
for df, tag in [(lcap, "lcap"), (mcap, "mcap"), (scap, "scap")]:
    for isin in df["isin_code"]:
        tag_map[isin] = tag

# Assign tag to bigsheet
bigsheet["tag"] = bigsheet["isin_code"].apply(lambda x: tag_map.get(x, "other"))

# Save with date in file name
today_str = datetime.now().strftime("%Y-%m-%d")
output_file = os.path.join(downloads_path, f"bigsheet_with_tags_{today_str}.xlsx")

bigsheet.to_excel(output_file, index=False)

print(f"✅ Final file saved at: {output_file}")
