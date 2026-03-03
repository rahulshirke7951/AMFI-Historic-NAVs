import pandas as pd
import sqlite3
import os
import requests
from io import BytesIO

# ---------- CONFIG ----------
DRIVE_FOLDER_LINK = "PASTE_YOUR_DRIVE_LINK_HERE"
OUTPUT_DB = "output/historic.db"
TABLE_NAME = "nav_history"

# For multiple files you can store direct download links here
FILE_LINKS = [
    # paste individual google drive file links here
]

# ---------- CREATE OUTPUT FOLDER ----------
os.makedirs("output", exist_ok=True)

# ---------- DB CONNECTION ----------
conn = sqlite3.connect(OUTPUT_DB)

first_file = True

def download_drive_file(url):
    print(f"Downloading: {url}")
    response = requests.get(url)
    return BytesIO(response.content)

# ---------- PROCESS FILES ----------
for file_url in FILE_LINKS:
    file_data = download_drive_file(file_url)

    df = pd.read_excel(file_data)

    df.columns = df.columns.str.lower()

    df = df[["scheme_code", "date", "nav"]]

    df.rename(columns={
        "date": "nav_date",
        "nav": "nav_value"
    }, inplace=True)

    df["nav_date"] = pd.to_datetime(df["nav_date"])
    df["nav_value"] = pd.to_numeric(df["nav_value"], errors="coerce")

    df.to_sql(
        TABLE_NAME,
        conn,
        if_exists="replace" if first_file else "append",
        index=False,
        chunksize=50000
    )

    first_file = False

# ---------- CREATE INDEX ----------
conn.execute("""
CREATE INDEX IF NOT EXISTS idx_scheme_date
ON nav_history (scheme_code, nav_date)
""")

conn.close()

print("✅ historic.db created successfully")
