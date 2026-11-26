import requests
import polars as pl
import time
from pathlib import Path

URL = "https://api.fdic.gov/banks/history"

FIELDS = (
    "CERT,INSTNAME,CHANGECODE,CHANGECODE_DESC,"
    "ORG_ROLE_CDE,EFFDATE,ACQDATE,PROCDATE,"
    "ACQ_UNINUM,OUT_UNINUM"
)

LIMIT = 10000
OUTPUT = Path("data/interim/institution_history.parquet")

session = requests.Session()

# Lấy tổng số bản ghi thỏa mãn bộ lọc
base_params = {
    "filters": "!(ORG_ROLE_CDE:BR)",
    "fields": FIELDS,
    "limit": 1,
}

print("Checking total number of records...")

response = session.get(URL, params=base_params, timeout=60)
response.raise_for_status()

expected_total = response.json().get("meta", {}).get("total", 0)

print("Expected records:", expected_total)

all_batches = []
offset = 0
downloaded = 0

while offset < expected_total:
    params = {
        "filters": "!(ORG_ROLE_CDE:BR)",
        "fields": FIELDS,
        "limit": LIMIT,
        "offset": offset,
    }

    for attempt in range(5):
        try:
            response = session.get(
                URL,
                params=params,
                timeout=120
            )
            response.raise_for_status()

            records = response.json().get("data", [])

            rows = [
                item.get("data", item)
                for item in records
            ]

            if not rows:
                break

            batch = pl.DataFrame(rows)
            all_batches.append(batch)

            downloaded += len(rows)
            print(
                f"Downloaded {downloaded:,} "
                f"/ {expected_total:,} records"
            )

            offset += len(rows)
            time.sleep(0.2)
            break

        except Exception as error:
            print(f"Attempt {attempt + 1}/5 failed: {error}")
            if attempt == 4:
                raise
            time.sleep(3)

    else:
        raise RuntimeError("Download failed after 5 attempts.")

    if not records:
        break

if not all_batches:
    raise RuntimeError("No records were downloaded.")

df = pl.concat(all_batches, how="diagonal_relaxed")

print("\nDownload finished.")
print("Expected records:", expected_total)
print("Downloaded records:", df.height)

if df.height != expected_total:
    raise ValueError(
        "Downloaded record count does not match API total."
    )

print("\nOrganization role distribution:")
print(
    df.group_by("ORG_ROLE_CDE")
    .len()
    .sort("len", descending=True)
)

print("\nChange code distribution:")
print(
    df.group_by(["CHANGECODE", "CHANGECODE_DESC"])
    .len()
    .sort("len", descending=True)
)

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
df.write_parquet(OUTPUT, compression="zstd")

print("\nSaved:", OUTPUT)
print("File size (MB):", round(OUTPUT.stat().st_size / (1024 * 1024), 2))
print("\nDOWNLOAD COMPLETED")