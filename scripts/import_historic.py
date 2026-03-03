import pandas as pd
import sqlite3
import requests
from io import BytesIO

# ===== CONFIG =====
OUTPUT_DB = "historic.db"
TABLE_NAME = "nav_history"

# Replace with direct Google Drive download links
FILE_LINKS = [
    "https://drive.google.com/uc?export=download&id=1ysjCgWoHF6u3-Z8kIKdZUIj7nK9KGb13"
]


# ===== DB CONNECTION =====
conn = sqlite3.connect(OUTPUT_DB)
first_file = True


def download_drive_file(url):
    print(f"Downloading: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return BytesIO(response.content)


for file_url in FILE_LINKS:
    file_data = download_drive_file(file_url)

    # Read Excel
    df = pd.read_excel(file_data, engine="openpyxl")

    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()

    # Validate required columns
    required_cols = ["scheme_code", "date", "nav"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required columns in file: {file_url}")

    # Keep only required columns
    df = df[required_cols]

    # Convert types properly
    df["scheme_code"] = df["scheme_code"].astype(str)

    df["nav_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    df["nav_value"] = pd.to_numeric(df["nav"], errors="coerce")

    # Drop original columns
    df = df[["scheme_code", "nav_date", "nav_value"]]

    # Drop invalid rows
    df = df.dropna(subset=["nav_date", "nav_value"])

    # Insert into DB
    df.to_sql(
        TABLE_NAME,
        conn,
        if_exists="replace" if first_file else "append",
        index=False,
        chunksize=50000
    )

    first_file = False

# Create index for performance
conn.execute("""
CREATE INDEX IF NOT EXISTS idx_scheme_date
ON nav_history (scheme_code, nav_date)
""")

conn.close()

print("✅ historic.db created successfully")
