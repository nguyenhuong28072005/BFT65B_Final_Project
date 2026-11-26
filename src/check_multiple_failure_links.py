
import polars as pl
from pathlib import Path

print("=" * 65)
print("CHECKING MULTIPLE FAILURE EVENTS AGAINST FINANCIALS")
print("=" * 65)

# Load failure records
failures = pl.read_parquet("data/raw/failures/failures.parquet")

failures = failures.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

# Identify CERTs with multiple failure records
multiple_certs = (
    failures
    .filter(pl.col("CERT").is_not_null())
    .group_by("CERT")
    .len()
    .filter(pl.col("len") > 1)
    .select("CERT")
)

multiple_failures = (
    failures
    .join(multiple_certs, on="CERT", how="inner")
    .select(["CERT", "FAILDATE_PARSED"])
    .sort(["CERT", "FAILDATE_PARSED"])
)

# Load Financials
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

financial_dfs = []

for file in files:
    df = pl.read_parquet(file).select(["CERT", "REPDTE"])
    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False),
        pl.col("REPDTE").cast(pl.String, strict=False)
    )
    financial_dfs.append(df)

financials = pl.concat(financial_dfs, how="vertical")

financials = financials.with_columns(
    pl.col("REPDTE").str.strptime(
        pl.Date, format="%Y%m%d", strict=False
    ).alias("REPORT_DATE")
)

# Summarize financial reporting history for each CERT
history = (
    financials
    .group_by("CERT")
    .agg(
        pl.col("REPORT_DATE").min().alias("FIRST_REPORT"),
        pl.col("REPORT_DATE").max().alias("LAST_REPORT"),
        pl.len().alias("FINANCIAL_RECORDS")
    )
)

# Compare every failure event with its financial reporting history
result = (
    multiple_failures
    .join(history, on="CERT", how="left")
    .sort(["CERT", "FAILDATE_PARSED"])
)

print("\nAll 88 failure events and their financial reporting histories:")
print(result)

print("\nSummary:")
print("Failure events:", result.height)
print("Events with matching financial history:",
      result.filter(pl.col("FIRST_REPORT").is_not_null()).height)
print("Events without matching financial history:",
      result.filter(pl.col("FIRST_REPORT").is_null()).height)

print("\n" + "=" * 65)
print("CHECK COMPLETED")
print("=" * 65)
