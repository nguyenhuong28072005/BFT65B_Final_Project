
import requests
import pandas as pd
from pathlib import Path

URL = "https://api.fdic.gov/banks/failures"

params = {
    "fields": "CERT,FIN,NAME,CITYST,FAILDATE,RESTYPE,RESTYPE1,COST,ID",
    "limit": 10000,
    "format": "json"
}

print("Downloading enriched FDIC failure records...")

response = requests.get(URL, params=params, timeout=120)
response.raise_for_status()

result = response.json()
expected = result["meta"]["total"]

records = [item["data"] for item in result["data"]]
df = pd.DataFrame(records)

print("\nExpected records:", expected)
print("Downloaded records:", len(df))

if len(df) != expected:
    raise ValueError("Record count does not match FDIC API total!")

output_dir = Path("data/raw/failures")
output_dir.mkdir(parents=True, exist_ok=True)

output_file = output_dir / "failures_enriched.parquet"
df.to_parquet(output_file, index=False)

print("\nSaved:", output_file)
print("\nColumns:")
print(df.columns.tolist())

print("\nEvent type (RESTYPE):")
print(df["RESTYPE"].value_counts(dropna=False))

print("\nDetailed event type (RESTYPE1):")
print(df["RESTYPE1"].value_counts(dropna=False))

print("\nSample records:")
print(
    df[
        ["CERT", "FIN", "NAME", "FAILDATE", "RESTYPE", "RESTYPE1"]
    ].head(10)
)

print("\nFile size (MB):", round(
    output_file.stat().st_size / (1024 * 1024), 2
))

print("\nDOWNLOAD COMPLETED")
