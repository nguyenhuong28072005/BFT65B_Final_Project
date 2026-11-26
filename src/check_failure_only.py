
import polars as pl

FILE_PATH = "data/raw/failures/failures_enriched.parquet"

df = pl.read_parquet(FILE_PATH)

df = df.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

# Keep only actual FAILURE events
failure_only = df.filter(pl.col("RESTYPE") == "FAILURE")

# Records with and without CERT
with_cert = failure_only.filter(pl.col("CERT").is_not_null())
without_cert = failure_only.filter(pl.col("CERT").is_null())

# Check repeated CERTs
per_cert = (
    with_cert
    .group_by("CERT")
    .len()
    .sort("len", descending=True)
)

multiple_cert = per_cert.filter(pl.col("len") > 1)

# Check duplicate CERT + failure date
duplicates = (
    with_cert
    .group_by(["CERT", "FAILDATE_PARSED"])
    .len()
    .filter(pl.col("len") > 1)
)

print("=" * 65)
print("FAILURE-ONLY DATA QUALITY CHECK")
print("=" * 65)

print("\n1. Total FAILURE events:", failure_only.height)
print("2. FAILURE events with CERT:", with_cert.height)
print("3. FAILURE events without CERT:", without_cert.height)

print("\n4. Unique CERT:", per_cert.height)
print("5. CERT with multiple FAILURE events:", multiple_cert.height)

print("\n6. Duplicate CERT + failure date groups:", duplicates.height)

print("\n7. Top CERTs with multiple FAILURE events:")
print(multiple_cert.head(15))

print("\n8. Failure event date range:")
print(
    failure_only.select(
        pl.col("FAILDATE_PARSED").min().alias("Earliest"),
        pl.col("FAILDATE_PARSED").max().alias("Latest")
    )
)

print("\n" + "=" * 65)
print("CHECK COMPLETED")
print("=" * 65)
