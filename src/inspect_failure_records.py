
import polars as pl
from pathlib import Path

FILE_PATH = Path("data/raw/failures/failures.parquet")

failures = pl.read_parquet(FILE_PATH)

failures = failures.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

print("=" * 65)
print("FAILURE RECORDS INVESTIGATION")
print("=" * 65)

print("\n1. Total failure records:", f"{failures.height:,}")

print("\n2. Records with missing CERT:")
missing_cert = failures.filter(pl.col("CERT").is_null())
print("Count:", f"{missing_cert.height:,}")
print(missing_cert.select(["FAILDATE", "COST", "ID"]).head(10))

print("\n3. Duplicate CERT + failure date:")
duplicates = (
    failures
    .filter(pl.col("CERT").is_not_null())
    .group_by(["CERT", "FAILDATE_PARSED"])
    .len()
    .filter(pl.col("len") > 1)
)
print("Duplicate groups:", duplicates.height)
print(duplicates.head(10))

print("\n4. Number of failure records per CERT:")
per_bank = (
    failures
    .filter(pl.col("CERT").is_not_null())
    .group_by("CERT")
    .len()
    .sort("len", descending=True)
)
print("Unique CERT:", per_bank.height)
print("Banks with multiple records:", per_bank.filter(pl.col("len") > 1).height)
print(per_bank.head(10))

print("\n5. Failure date range:")
print(
    failures.select(
        pl.col("FAILDATE_PARSED").min().alias("Earliest date"),
        pl.col("FAILDATE_PARSED").max().alias("Latest date")
    )
)

print("\n" + "=" * 65)
print("INVESTIGATION COMPLETED")
print("=" * 65)
