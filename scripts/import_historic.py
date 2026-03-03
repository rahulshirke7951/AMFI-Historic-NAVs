import pandas as pd
import sqlite3
import requests
from io import BytesIO
import os

# ==========================
# CONFIG
# ==========================
OUTPUT_DB = "output/historic.db"
TABLE_NAME = "nav_history"

# Add your direct Google Drive download links here
FILE_LINKS = [
    "https://drive.google.com/uc?export=download&id=1ysjCgWoHF6u3-Z8kIKdZUIj7nK9KGb13"
 ]

# Ensure output folder exists
os.makedirs("output", exist_ok=True)

# ==========================
# DB CONNECTION
# ==========================
conn = sqlite3.connect(OUTPUT_DB)
cursor = conn.cursor()

# Create table with PRIMARY KEY (prevents duplicates)
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    scheme_code TEXT,
    nav_date DATE,
    nav_value REAL,
    PRIMARY KEY (scheme_code, nav_date)
)
""")
conn.commit()


def download_drive_file(url):
    print(f"Downloading: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return BytesIO(response.content)


# ==========================
# PROCESS FILES
# ==========================
for file_url in FILE_LINKS:
    print(f"Processing: {file_url}")

    file_data = download_drive_file(file_url)
    df = pd.read_excel(file_data, engine="openpyxl")

    df.columns = df.columns.str.strip().str.lower()

    df = df[["scheme_code", "date", "nav"]]

    df["scheme_code"] = df["scheme_code"].astype(str)
    df["nav_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["nav_value"] = pd.to_numeric(df["nav"], errors="coerce")

    df = df[["scheme_code", "nav_date", "nav_value"]]
    df = df.dropna()

    # Insert rows with duplicate protection
    for row in df.itertuples(index=False):
        cursor.execute(f"""
            INSERT OR IGNORE INTO {TABLE_NAME}
            (scheme_code, nav_date, nav_value)
            VALUES (?, ?, ?)
        """, (row.scheme_code, row.nav_date, row.nav_value))

    conn.commit()

# Create performance index
cursor.execute(f"""
CREATE INDEX IF NOT EXISTS idx_scheme_date
ON {TABLE_NAME} (scheme_code, nav_date)
""")

conn.commit()
conn.close()

print("✅ historic.db created successfully")
