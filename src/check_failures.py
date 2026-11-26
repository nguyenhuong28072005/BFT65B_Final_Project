
import polars as pl
from pathlib import Path

# 1. Load Failures data
FILE_PATH = Path("data/raw/failures/failures.parquet")

df = pl.read_parquet(FILE_PATH)

print("=" * 60)
print("FAILURES DATA QUALITY CHECK")
print("=" * 60)

# 2. Total records
print("\nTotal records:", f"{df.height:,}")

# 3. Data schema
print("\nData schema:")
print(df.schema)

# 4. Missing values
print("\nMissing values by column:")

for column in df.columns:
    missing_count = df[column].null_count()
    missing_pct = missing_count / df.height * 100

    print(
        f"{column:12s}: "
        f"{missing_count:>6,} missing "
        f"({missing_pct:.2f}%)"
    )

# 5. Duplicate records
print("\nDuplicate ID records:",
      df.height - df["ID"].n_unique())

# 6. Date range
df = df.with_columns(
    pl.col("FAILDATE").str.to_date(
        format="%m/%d/%Y",
        strict=False
    ).alias("FAILDATE_PARSED")
)

print("\nFailure date range:")
print(
    df.select(
        pl.col("FAILDATE_PARSED").min().alias("Earliest date"),
        pl.col("FAILDATE_PARSED").max().alias("Latest date")
    )
)

# 7. Sample records
print("\nSample records:")
print(df.head(5))

print("\n" + "=" * 60)
print("FAILURES DATA QUALITY CHECK COMPLETED")
print("=" * 60)
