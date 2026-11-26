
import polars as pl

df = pl.read_parquet("data/raw/failures/failures_enriched.parquet")

df = df.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

# Keep actual failure events with a valid CERT
failure_only = df.filter(
    (pl.col("RESTYPE") == "FAILURE") &
    (pl.col("CERT").is_not_null())
)

# Identify CERTs appearing more than once
repeated = (
    failure_only
    .group_by("CERT")
    .len()
    .filter(pl.col("len") > 1)
    .select("CERT")
)

details = (
    failure_only
    .join(repeated, on="CERT", how="inner")
    .sort(["CERT", "FAILDATE_PARSED"])
    .select([
        "CERT",
        "NAME",
        "FAILDATE_PARSED",
        "RESTYPE1",
        "COST",
        "ID"
    ])
)

print("=" * 65)
print("REPEATED FAILURE EVENTS - DETAILED INSPECTION")
print("=" * 65)

print("\nRepeated CERT records:")
print(details)

print("\nNumber of distinct failure dates per CERT:")
print(
    details
    .group_by("CERT")
    .agg(
        pl.col("FAILDATE_PARSED").n_unique().alias("Distinct dates"),
        pl.col("NAME").n_unique().alias("Distinct names")
    )
    .sort("CERT")
)

print("\n" + "=" * 65)
print("INSPECTION COMPLETED")
print("=" * 65)
