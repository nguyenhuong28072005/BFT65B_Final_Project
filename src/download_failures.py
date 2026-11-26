
import requests
import pandas as pd
from pathlib import Path

# 1. FDIC API settings
URL = "https://api.fdic.gov/banks/failures"

params = {
    "fields": "CERT,FAILDATE,COST",
    "limit": 10000,
    "format": "json"
}

# 2. Output folder
OUTPUT_DIR = Path("data/raw/failures")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = OUTPUT_DIR / "failures.parquet"

# 3. Download data
print("Connecting to FDIC Failures API...")

response = requests.get(
    URL,
    params=params,
    timeout=120
)

response.raise_for_status()
result = response.json()

# 4. Extract records
total_records = result["meta"]["total"]
records = result["data"]

data = [item["data"] for item in records]

df = pd.DataFrame(data)

# 5. Verify record count
print("\nExpected records:", total_records)
print("Downloaded records:", len(df))

if len(df) != total_records:
    raise ValueError("Downloaded record count does not match API total!")

# 6. Save as Parquet
df.to_parquet(
    OUTPUT_FILE,
    index=False
)

print("\nDownload completed!")
print("Saved file:", OUTPUT_FILE)
print("Columns:", list(df.columns))
print("\nFirst 3 records:")
print(df.head(3))

print("\nFile size (MB):", round(
    OUTPUT_FILE.stat().st_size / (1024 * 1024), 2
))
