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

FILE_LINKS = [
    "https://drive.google.com/uc?export=download&id=1ysjCgWoHF6u3-Z8kIKdZUIj7nK9KGb13",
    "https://drive.google.com/uc?export=download&id=1nSVx7v26gPdNThbHfWdI1BDrN7iP2TTl",
    "https://drive.google.com/uc?export=download&id=1cdP7LmEwiUMwBiowRsqyPFnTWq1QrDPL",
    "https://drive.google.com/uc?export=download&id=1_b0CW3ANmK48B0CHC_cILr60jsiTW0WM",
    "https://drive.google.com/uc?export=download&id=1U-MKmBaenEZ2387ZyOSd6tysTEXa6fF9",
    "https://drive.google.com/uc?export=download&id=1KWDQ7VZ1Z0GDC9x4mU8i_A6FpXQU_akH",
    "https://drive.google.com/uc?export=download&id=105mZhHLtRaEHNuJb2ss4Fca6usKNE4Sk",
    "https://drive.google.com/uc?export=download&id=1xEUyXfM9lwPpntcqSW4g18YdbyKzujml",
    "https://drive.google.com/uc?export=download&id=1txRxy1tj7q3Tk4trD5Yi53X35yRDbOBz",
    "https://drive.google.com/uc?export=download&id=1Tv09iATannfz_OfnFbZ58xgDwjjT_RzS",
    "https://drive.google.com/uc?export=download&id=1IpHBwDqyWsrw7EagiEJ7LWekKDX-cy1n",
    "https://drive.google.com/uc?export=download&id=15Okp9f-SXxxK5ag26Jufzr1GhUVlmdPG",
    "https://drive.google.com/uc?export=download&id=10gUd8-ARhHauvHz9g7kh24vdVLlBY4Q1"
]

# Ensure output folder exists
os.makedirs("output", exist_ok=True)

# ==========================
# DOWNLOAD FUNCTION
# ==========================
def download_drive_file(url):
    response = requests.get(url)
    response.raise_for_status()

    # Detect HTML instead of file (Drive permission issue)
    if b'<!DOCTYPE html>' in response.content[:100]:
        raise Exception(
            f"Google Drive returned HTML instead of file. "
            f"Check sharing permissions for: {url}"
        )

    return BytesIO(response.content)


# ==========================
# DATABASE PROCESS
# ==========================
with sqlite3.connect(OUTPUT_DB) as conn:

    # Create table with PRIMARY KEY (duplicate protection)
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        scheme_code TEXT,
        nav_date DATE,
        nav_value REAL,
        PRIMARY KEY (scheme_code, nav_date)
    )
    """)

    total_rows = 0

    for file_url in FILE_LINKS:
        try:
            print(f"Processing: {file_url}")

            file_data = download_drive_file(file_url)
            df = pd.read_excel(file_data, engine="openpyxl")

            # Standardize columns
            df.columns = df.columns.str.strip().str.lower()

            required_cols = {"scheme_code", "date", "nav"}
            if not required_cols.issubset(df.columns):
                print(f"❌ Missing columns in file. Found: {df.columns.tolist()}")
                continue

            # Clean & transform
            df["scheme_code"] = df["scheme_code"].astype(str)
            df["nav_date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
            df["nav_value"] = pd.to_numeric(df["nav"], errors="coerce")

            df = df[["scheme_code", "nav_date", "nav_value"]].dropna()

            row_count = len(df)
            total_rows += row_count

            print(f"Inserting {row_count} rows...")

            # Bulk insert via temp table (fast)
            df.to_sql("temp_table", conn, if_exists="replace", index=False)

            conn.execute(f"""
                INSERT OR IGNORE INTO {TABLE_NAME}
                (scheme_code, nav_date, nav_value)
                SELECT scheme_code, nav_date, nav_value
                FROM temp_table
            """)

            conn.commit()

        except Exception as e:
            print(f"❌ Error processing {file_url}: {e}")

    # Create index for performance
    conn.execute(f"""
    CREATE INDEX IF NOT EXISTS idx_scheme_date
    ON {TABLE_NAME} (scheme_code, nav_date)
    """)

print(f"✅ historic.db updated successfully. Total rows processed: {total_rows}")
