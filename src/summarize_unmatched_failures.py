
import polars as pl
from pathlib import Path

# Load failures
failures = pl.read_parquet("data/raw/failures/failures.parquet")

failures = failures.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

# Load all CERT identifiers from Financials
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

financial_dfs = []

for file in files:
    df = pl.read_parquet(file).select("CERT")
    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False)
    )
    financial_dfs.append(df)

financials = pl.concat(financial_dfs, how="vertical")

known_certs = financials.select("CERT").drop_nulls().unique()

# Find unmatched failure events
unmatched = (
    failures
    .filter(pl.col("CERT").is_not_null())
    .join(known_certs, on="CERT", how="anti")
    .with_columns(
        pl.col("FAILDATE_PARSED").dt.year().alias("FAILURE_YEAR")
    )
)

# Separate events before and during the Financials period
cutoff_date = pl.date(1984, 3, 31)

before_period = unmatched.filter(
    pl.col("FAILDATE_PARSED") < cutoff_date
)

during_period = unmatched.filter(
    pl.col("FAILDATE_PARSED") >= cutoff_date
)

print("=" * 65)
print("SUMMARY OF UNMATCHED FAILURE EVENTS")
print("=" * 65)

print("\nTotal unmatched events:", unmatched.height)
print("Unique unmatched CERT:", unmatched["CERT"].n_unique())

print("\n1. Events before Financials starts (before 1984-03-31):")
print("Records:", before_period.height)
print("Unique CERT:", before_period["CERT"].n_unique())

print("\n2. Events on/after 1984-03-31:")
print("Records:", during_period.height)
print("Unique CERT:", during_period["CERT"].n_unique())

print("\n3. Unmatched events by year:")
print(
    unmatched
    .group_by("FAILURE_YEAR")
    .agg(
        pl.len().alias("EVENTS"),
        pl.col("CERT").n_unique().alias("UNIQUE_CERT")
    )
    .sort("FAILURE_YEAR")
)

print("\n4. Sample unmatched events during Financials period:")
print(
    during_period
    .select(["CERT", "FAILDATE_PARSED", "COST", "ID"])
    .head(20)
)

print("\n" + "=" * 65)
print("SUMMARY COMPLETED")
print("=" * 65)
