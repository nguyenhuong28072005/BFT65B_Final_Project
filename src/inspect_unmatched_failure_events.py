
import polars as pl
from pathlib import Path

# Load failure data
failures = pl.read_parquet("data/raw/failures/failures.parquet")

failures = failures.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

# Load financial bank identifiers
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

financial_certs = []

for file in files:
    df = pl.read_parquet(file).select("CERT")
    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False)
    )
    financial_certs.append(df)

financials = pl.concat(financial_certs, how="vertical")

known_certs = financials.select("CERT").drop_nulls().unique()

# Identify failure events whose CERT does not exist in Financials
unmatched = (
    failures
    .filter(pl.col("CERT").is_not_null())
    .join(known_certs, on="CERT", how="anti")
    .sort("FAILDATE_PARSED")
)

print("=" * 65)
print("FAILURE EVENTS WITHOUT FINANCIAL RECORDS")
print("=" * 65)

print("\nNumber of unmatched events:", unmatched.height)
print("\nUnmatched failure events:")
print(
    unmatched.select([
        "CERT",
        "FAILDATE_PARSED",
        "COST",
        "ID"
    ])
)

print("\n" + "=" * 65)
print("INSPECTION COMPLETED")
print("=" * 65)
