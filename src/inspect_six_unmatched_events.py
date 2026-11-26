
import polars as pl
from pathlib import Path

# 1. Load enriched failure events
events = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
)

events = events.with_columns([
    pl.col("CERT").cast(pl.Int64),
    pl.col("FAILDATE")
    .cast(pl.String)
    .str.strptime(
        pl.Date,
        format="%m/%d/%Y",
        strict=False
    )
    .alias("FAILDATE_PARSED")
])

# 2. Get all CERTs in Financials
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

cert_parts = []

for file in files:
    part = (
        pl.read_parquet(file)
        .select(pl.col("CERT").cast(pl.Int64))
        .filter(pl.col("CERT").is_not_null())
    )
    cert_parts.append(part)

financial_certs = (
    pl.concat(cert_parts, how="vertical")
    .unique()
)

# 3. Find unmatched events on/after Financials start
unmatched = (
    events
    .filter(
        pl.col("CERT").is_not_null()
        & (pl.col("FAILDATE_PARSED") >= pl.date(1984, 3, 31))
    )
    .join(
        financial_certs,
        on="CERT",
        how="anti"
    )
    .sort("FAILDATE_PARSED")
)

# 4. Display detailed records
print("=" * 70)
print("UNMATCHED EVENTS ON/AFTER 1984-03-31")
print("=" * 70)

print("\nNumber of unmatched events:", unmatched.height)

columns = [
    "CERT",
    "NAME",
    "CITYST",
    "FAILDATE_PARSED",
    "RESTYPE",
    "RESTYPE1",
    "FIN",
    "COST",
    "ID"
]

print("\nDetailed events:")
print(unmatched.select(columns))

print("\nEvent type distribution:")
print(
    unmatched.group_by("RESTYPE")
    .len()
    .sort("RESTYPE")
)

print("\nCHECK COMPLETED")
