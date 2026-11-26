
import requests
import pandas as pd
from pathlib import Path

URL = "https://api.fdic.gov/banks/institutions"
LIMIT = 10000

OUTPUT_DIR = Path("data/raw/institutions")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "institutions.parquet"

all_records = []
offset = 0

print("Connecting to FDIC Institutions API...")

while True:
    params = {
        "fields": "CERT,STNAME,ACTIVE,CHANGEC1",
        "limit": LIMIT,
        "offset": offset,
        "format": "json"
    }

    response = requests.get(URL, params=params, timeout=120)
    response.raise_for_status()

    result = response.json()

    total_records = result["meta"]["total"]
    records = result["data"]

    batch = [item["data"] for item in records]
    all_records.extend(batch)

    print(
        f"Downloaded {len(all_records):,} / {total_records:,} records"
    )

    if len(records) == 0 or len(all_records) >= total_records:
        break

    offset += LIMIT

df = pd.DataFrame(all_records)

print("\nExpected records:", total_records)
print("Downloaded records:", len(df))

if len(df) != total_records:
    raise ValueError(
        "Downloaded record count does not match API total!"
    )

df.to_parquet(OUTPUT_FILE, index=False)

print("\nDownload completed!")
print("Saved file:", OUTPUT_FILE)
print("Columns:", list(df.columns))

print("\nFirst 5 records:")
print(df.head(5))

print(
    "\nFile size (MB):",
    round(OUTPUT_FILE.stat().st_size / (1024 * 1024), 2)
)
