import requests
import polars as pl

URL = "https://api.fdic.gov/banks/history"

params = {
    "filters": '!(ORG_ROLE_CDE:BR)',
    "fields": (
        "CERT,INSTNAME,CHANGECODE,CHANGECODE_DESC,"
        "ORG_ROLE_CDE,EFFDATE,ACQDATE,PROCDATE,"
        "ACQ_UNINUM,OUT_UNINUM"
    ),
    "limit": 10000
}

print("Requesting non-branch History records...")

response = requests.get(URL, params=params, timeout=60)
print("HTTP status:", response.status_code)
response.raise_for_status()

result = response.json()
records = result.get("data", [])
total = result.get("meta", {}).get("total", 0)

print("Total matching records in API:", total)
print("Records downloaded:", len(records))

if len(records) == 0:
    print("\nNo non-branch records returned.")
else:
    rows = [
        item.get("data", item)
        for item in records
    ]

    df = pl.DataFrame(rows)

    print("\n1. ORGANIZATION ROLE DISTRIBUTION")
    print(
        df.group_by("ORG_ROLE_CDE")
        .len()
        .sort("len", descending=True)
    )

    print("\n2. CHANGE CODE DISTRIBUTION")
    print(
        df.group_by(
            ["CHANGECODE", "CHANGECODE_DESC"]
        )
        .len()
        .sort("len", descending=True)
    )

    print("\n3. SAMPLE RECORDS")
    print(df.head(10))

    output = "data/interim/non_branch_history_sample.parquet"
    df.write_parquet(output)

    print("\nSaved:", output)

print("\nCHECK COMPLETED")