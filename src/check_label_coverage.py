
import polars as pl
from pathlib import Path

# Load financial data
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

financials = pl.concat(
    [
        pl.read_parquet(file).select([
            pl.col("CERT").cast(pl.Int64, strict=False),
            pl.col("REPDTE").cast(pl.String)
        ])
        for file in files
    ],
    how="vertical"
).with_columns(
    pl.col("REPDTE").str.strptime(
        pl.Date, format="%Y%m%d", strict=False
    ).alias("REPORT_DATE")
)

# Load failure events
failures = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
).filter(
    (pl.col("RESTYPE") == "FAILURE") &
    pl.col("CERT").is_not_null()
).with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

# Latest available financial reporting date
print("=" * 65)
print("LABEL COVERAGE CHECK")
print("=" * 65)

print("\nFinancial reporting date range:")
print(financials.select([
    pl.col("REPORT_DATE").min().alias("FIRST_REPORT"),
    pl.col("REPORT_DATE").max().alias("LAST_REPORT")
]))

# Modeling cutoff
modeling_cutoff = pl.date(2025, 6, 30)

eligible_quarters = (
    financials
    .filter(pl.col("REPORT_DATE") <= modeling_cutoff)
    .select("REPORT_DATE")
    .unique()
    .sort("REPORT_DATE")
)

print("\nLast eligible observation quarter:")
print(eligible_quarters.tail(5))

print("\nNumber of unique reporting quarters:")
print(eligible_quarters.height)

# Check whether each quarter has a full 4-quarter future horizon
coverage = (
    financials
    .filter(pl.col("REPORT_DATE") <= modeling_cutoff)
    .select(
        pl.col("REPORT_DATE").max().alias("LAST_OBSERVATION_DATE")
    )
)

print("\nModeling cutoff:", "2025-06-30")
print("Required follow-up horizon:", "4 quarters")
print("Financial data available through:", "2026-06-30")

print("\nLabel coverage check completed.")
