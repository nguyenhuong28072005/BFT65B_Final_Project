
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

multiple_certs = (
    failures
    .filter(pl.col("CERT").is_not_null())
    .group_by("CERT")
    .len()
    .filter(pl.col("len") > 1)
    .select("CERT")
)

details = (
    failures
    .join(multiple_certs, on="CERT", how="inner")
    .sort(["CERT", "FAILDATE_PARSED"])
    .select(["CERT", "FAILDATE_PARSED", "COST", "ID"])
)

print("=" * 65)
print("INVESTIGATION OF MULTIPLE FAILURE RECORDS")
print("=" * 65)

print("\nNumber of CERT with multiple records:", multiple_certs.height)

print("\nAll records for these CERT:")
print(details)

print("\nNumber of distinct failure dates per CERT:")
date_counts = (
    details
    .group_by("CERT")
    .agg(
        pl.col("FAILDATE_PARSED").n_unique().alias("Distinct dates"),
        pl.len().alias("Total records")
    )
    .sort("CERT")
)

print(date_counts)

print("\n" + "=" * 65)
print("INVESTIGATION COMPLETED")
print("=" * 65)
