import json
import time
from pathlib import Path

import pandas as pd
import requests


# 1. API settings
URL = "https://api.fdic.gov/banks/financials"

FIELDS = (
    "CERT,REPDTE,ASSET,DEP,EQ,NETINC,"
    "ROA,NCLNLS,LNATRES,RBC1AAJ"
)

FILTERS = "REPDTE:[19840331 TO 20260630]"
LIMIT = 10000

# 2. Output folder
OUTPUT_DIR = Path("data/raw/financials_parts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = OUTPUT_DIR / "download_progress.json"

# 3. Download settings
session = requests.Session()

# Check whether a previous download can be resumed
if MANIFEST_PATH.exists():
    with open(MANIFEST_PATH, "r", encoding="utf-8") as file:
        progress = json.load(file)

    if (
        progress["filters"] != FILTERS
        or progress["fields"] != FIELDS
        or progress["limit"] != LIMIT
    ):
        raise ValueError(
            "Download settings do not match the existing progress file."
        )

    offset = progress["next_offset"]
    total = progress["total"]

    print("Resuming previous download...")
    print("Next offset:", offset)

else:
    offset = 0
    total = None

    print("Starting a new download...")


# 4. Download data in batches
while total is None or offset < total:

    params = {
    "sort_by": "REPDTE,CERT",  # Thêm CERT làm khóa phụ để thứ tự trả về là duy nhất, không bị sót dòng giữa 2 trang
    "sort_order": "ASC",
    "limit": limit,
    "offset": offset
}

    print(f"\nDownloading records from offset {offset}...")

    # Retry up to 5 times if the request fails
    for attempt in range(5):
        try:
            response = session.get(
                URL,
                params=params,
                timeout=120
            )
            response.raise_for_status()
            result = response.json()
            break

        except (requests.exceptions.RequestException, ValueError) as error:
            if attempt == 4:
                raise RuntimeError(
                    "Download failed after 5 attempts."
                ) from error

            wait_time = 2 ** (attempt + 1)
            print(f"Request failed. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)

    # Read total number of matching records
    if total is None:
        total = result["meta"]["total"]

    records = result["data"]
    batch_count = len(records)

    if batch_count == 0:
        break

    # Convert the batch into a DataFrame
    df = pd.DataFrame(
        [item["data"] for item in records]
    )

    # Save this batch as a Parquet file
    output_file = OUTPUT_DIR / f"financials_offset_{offset}.parquet"
    df.to_parquet(output_file, index=False)

    # Update progress only after the batch is saved
    offset += batch_count

    progress = {
        "filters": FILTERS,
        "fields": FIELDS,
        "limit": LIMIT,
        "next_offset": offset,
        "total": total
    }

    temporary_manifest = OUTPUT_DIR / "download_progress.tmp"

    with open(temporary_manifest, "w", encoding="utf-8") as file:
        json.dump(progress, file, indent=4)

    temporary_manifest.replace(MANIFEST_PATH)

    print(f"Saved: {output_file}")
    print(f"Progress: {offset:,} / {total:,} records")

    time.sleep(0.2)


# 5. Final summary
print("\nDownload completed!")
print(f"Total records downloaded: {offset:,}")
print(f"Expected total records: {total:,}")
print(f"Output folder: {OUTPUT_DIR}")