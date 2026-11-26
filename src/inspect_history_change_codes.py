
import requests
import polars as pl

URL = "https://api.fdic.gov/banks/history"

# Chỉ lấy các trường cần thiết để kiểm tra mã sự kiện
fields = [
    "CERT",
    "INSTNAME",
    "CHANGECODE",
    "CHANGECODE_DESC",
    "ORG_ROLE_CDE",
    "EFFDATE",
    "ACQDATE",
    "PROCDATE",
    "ACQ_UNINUM",
    "OUT_UNINUM"
]

params = {
    "limit": 10000,
    "offset": 0,
    "format": "json",
    "fields": ",".join(fields)
}

print("Requesting FDIC History records...")

response = requests.get(URL, params=params, timeout=120)
print("HTTP status:", response.status_code)
response.raise_for_status()

result = response.json()
records = result.get("data", [])

print("Total records in API:", result.get("meta", {}).get("total"))
print("Records downloaded:", len(records))

# API records are wrapped inside the "data" key
rows = [record["data"] for record in records]

df = pl.DataFrame(rows)

print("\n" + "=" * 70)
print("1. AVAILABLE COLUMNS")
print("=" * 70)
print(df.columns)

print("\n" + "=" * 70)
print("2. CHANGE CODE DISTRIBUTION")
print("=" * 70)

print(
    df.group_by(["CHANGECODE", "CHANGECODE_DESC"])
    .agg(pl.len().alias("NUMBER_OF_RECORDS"))
    .sort("NUMBER_OF_RECORDS", descending=True)
)

print("\n" + "=" * 70)
print("3. ORGANIZATION ROLE DISTRIBUTION")
print("=" * 70)

print(
    df.group_by("ORG_ROLE_CDE")
    .agg(pl.len().alias("NUMBER_OF_RECORDS"))
    .sort("NUMBER_OF_RECORDS", descending=True)
)

print("\n" + "=" * 70)
print("4. CHANGE CODES BY ORGANIZATION ROLE")
print("=" * 70)

print(
    df.group_by([
        "ORG_ROLE_CDE",
        "CHANGECODE",
        "CHANGECODE_DESC"
    ])
    .agg(pl.len().alias("NUMBER_OF_RECORDS"))
    .sort("NUMBER_OF_RECORDS", descending=True)
)

# Save the inspection sample
from pathlib import Path

output_folder = Path("data/interim")
output_folder.mkdir(parents=True, exist_ok=True)

df.write_parquet(
    output_folder / "history_code_inspection_sample.parquet"
)

print("\nSaved: data/interim/history_code_inspection_sample.parquet")
print("CHECK COMPLETED")
