
import polars as pl
from pathlib import Path

# 1. Locate downloaded Parquet files
DATA_DIR = Path("data/raw/financials_parts")
files = sorted(DATA_DIR.glob("financials_offset_*.parquet"))

print("=" * 60)
print("FINANCIALS DATA QUALITY CHECK")
print("=" * 60)

print("\nNumber of Parquet files:", len(files))

if not files:
    raise FileNotFoundError("No Parquet files found!")

# 2. Read and standardize each file
dataframes = []

for file in files:
    df = pl.read_parquet(file)

    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False),
        pl.col("REPDTE").cast(pl.String, strict=False),
        pl.col("ASSET").cast(pl.Float64, strict=False),
        pl.col("DEP").cast(pl.Float64, strict=False),
        pl.col("EQ").cast(pl.Float64, strict=False),
        pl.col("NETINC").cast(pl.Float64, strict=False),
        pl.col("ROA").cast(pl.Float64, strict=False),
        pl.col("NCLNLS").cast(pl.Float64, strict=False),
        pl.col("LNATRES").cast(pl.Float64, strict=False),
        pl.col("RBC1AAJ").cast(pl.Float64, strict=False),
        pl.col("ID").cast(pl.String, strict=False)
    )

    dataframes.append(df)

# 3. Combine all files
financials = pl.concat(dataframes, how="vertical")

# 4. Check total records
print("\nTotal records:", f"{financials.height:,}")

# 5. Check date range
date_range = financials.select(
    pl.col("REPDTE").min().alias("Earliest date"),
    pl.col("REPDTE").max().alias("Latest date")
)

print("\nDate range:")
print(date_range)

# 6. Check duplicate bank-quarter records
duplicate_count = (
    financials
    .group_by(["CERT", "REPDTE"])
    .len()
    .filter(pl.col("len") > 1)
    .height
)

print("\nDuplicate bank-quarter groups:", duplicate_count)

# 7. Check missing values by column
missing_report = financials.select(
    [
        pl.col(column).null_count().alias(column)
        for column in financials.columns
    ]
)

print("\nMissing values by column:")
for column in financials.columns:
    missing_count = missing_report[column][0]
    missing_pct = missing_count / financials.height * 100

    print(
        f"{column:12s}: "
        f"{missing_count:>8,} missing "
        f"({missing_pct:.4f}%)"
    )

# 8. Check final schema
print("\nFinal data schema:")
print(financials.schema)

print("\n" + "=" * 60)
print("FINANCIALS DATA QUALITY CHECK COMPLETED")
print("=" * 60)
