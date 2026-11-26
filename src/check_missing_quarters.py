
import polars as pl
from pathlib import Path

print("=" * 65)
print("MISSING BANK-QUARTER INVESTIGATION")
print("=" * 65)

# 1. Load Financials
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

dataframes = []

for file in files:
    df = pl.read_parquet(file)

    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False),
        pl.col("REPDTE").cast(pl.String, strict=False)
    )

    dataframes.append(df.select(["CERT", "REPDTE"]))

financials = pl.concat(dataframes, how="vertical")

# 2. Convert dates and create quarter index
financials = financials.with_columns(
    pl.col("REPDTE").str.strptime(
        pl.Date, format="%Y%m%d", strict=False
    ).alias("REPORT_DATE")
).with_columns(
    (
        pl.col("REPORT_DATE").dt.year() * 4
        + pl.col("REPORT_DATE").dt.quarter()
    ).alias("QUARTER_INDEX")
)

# 3. Summarize each bank's observed quarters
bank_summary = financials.group_by("CERT").agg(
    pl.col("QUARTER_INDEX").min().alias("FIRST_QUARTER"),
    pl.col("QUARTER_INDEX").max().alias("LAST_QUARTER"),
    pl.len().alias("OBSERVED_QUARTERS")
)

# 4. Calculate expected quarters within each bank's date range
bank_summary = bank_summary.with_columns(
    (
        pl.col("LAST_QUARTER")
        - pl.col("FIRST_QUARTER")
        + 1
    ).alias("EXPECTED_QUARTERS")
).with_columns(
    (
        pl.col("EXPECTED_QUARTERS")
        - pl.col("OBSERVED_QUARTERS")
    ).alias("MISSING_QUARTERS")
)

# 5. Summarize missing quarters
print("\n1. OVERALL MISSING-QUARTER SUMMARY")

print(
    bank_summary.select(
        pl.col("CERT").count().alias("Total banks"),
        (pl.col("MISSING_QUARTERS") > 0)
        .sum().alias("Banks with missing quarters"),
        pl.col("MISSING_QUARTERS").sum()
        .alias("Total missing quarters"),
        pl.col("MISSING_QUARTERS").max()
        .alias("Maximum missing quarters for one bank")
    )
)

# 6. Show banks with the most missing quarters
print("\n2. BANKS WITH MOST MISSING QUARTERS")

print(
    bank_summary
    .filter(pl.col("MISSING_QUARTERS") > 0)
    .sort("MISSING_QUARTERS", descending=True)
    .head(20)
)

# 7. Distribution of missing quarters
print("\n3. MISSING-QUARTER DISTRIBUTION")

print(
    bank_summary
    .group_by("MISSING_QUARTERS")
    .len()
    .sort("MISSING_QUARTERS")
)

print("\n" + "=" * 65)
print("MISSING-QUARTER CHECK COMPLETED")
print("=" * 65)
