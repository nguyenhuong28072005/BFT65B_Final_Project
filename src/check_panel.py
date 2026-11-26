
import polars as pl
from pathlib import Path

print("=" * 65)
print("FINANCIALS BANK-QUARTER PANEL CHECK")
print("=" * 65)

# 1. Load Financials files
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

# 2. Convert reporting date into a date
financials = financials.with_columns(
    pl.col("REPDTE").str.strptime(
        pl.Date,
        format="%Y%m%d",
        strict=False
    ).alias("REPORT_DATE")
)

# 3. Remove records without bank ID or reporting date
valid = financials.filter(
    pl.col("CERT").is_not_null()
    & pl.col("REPORT_DATE").is_not_null()
)

# 4. Create quarter labels
valid = valid.with_columns(
    pl.col("REPORT_DATE").dt.year().alias("YEAR"),
    pl.col("REPORT_DATE").dt.quarter().alias("QUARTER")
)

# 5. Count records and unique banks
print("\n1. BASIC PANEL INFORMATION")
print("Total Financials records:", f"{financials.height:,}")
print("Records with valid CERT and date:", f"{valid.height:,}")
print("Unique banks:", f"{valid['CERT'].n_unique():,}")

# 6. Check duplicate bank-quarter observations
duplicate_groups = (
    valid.group_by(["CERT", "YEAR", "QUARTER"])
    .len()
    .filter(pl.col("len") > 1)
)

print("\n2. DUPLICATE BANK-QUARTER GROUPS")
print(f"{duplicate_groups.height:,}")

# 7. Count quarters per bank
bank_summary = valid.group_by("CERT").agg(
    pl.len().alias("Number_of_records"),
    pl.col("REPORT_DATE").min().alias("First_report"),
    pl.col("REPORT_DATE").max().alias("Last_report")
)

print("\n3. RECORDS PER BANK")
print(bank_summary.select(
    pl.col("Number_of_records").min().alias("Minimum"),
    pl.col("Number_of_records").median().alias("Median"),
    pl.col("Number_of_records").mean().alias("Mean"),
    pl.col("Number_of_records").max().alias("Maximum")
))

# 8. Overall reporting date range
print("\n4. REPORTING DATE RANGE")
print(valid.select(
    pl.col("REPORT_DATE").min().alias("Earliest"),
    pl.col("REPORT_DATE").max().alias("Latest")
))

# 9. Reporting records by year
print("\n5. RECORDS BY YEAR")
print(
    valid.group_by("YEAR")
    .len()
    .sort("YEAR")
)

# 10. Sample bank-level summary
print("\n6. SAMPLE BANK SUMMARY")
print(bank_summary.sort("Number_of_records", descending=True).head(10))

print("\n" + "=" * 65)
print("BANK-QUARTER PANEL CHECK COMPLETED")
print("=" * 65)
