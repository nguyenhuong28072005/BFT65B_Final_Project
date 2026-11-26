
import polars as pl

file = "data/raw/failures/failures_enriched.parquet"

df = pl.read_parquet(file)

print("=" * 65)
print("VERIFY FAILURE COUNTS")
print("=" * 65)

print("\n1. Total records:", df.height)

print("\n2. RESTYPE distribution:")
print(
    df.group_by("RESTYPE")
    .len()
    .sort("len", descending=True)
)

print("\n3. CERT missing by RESTYPE:")
print(
    df.group_by("RESTYPE")
    .agg(
        pl.col("CERT").is_null().sum().alias("MISSING_CERT"),
        pl.col("CERT").is_not_null().sum().alias("NON_MISSING_CERT")
    )
)

failure = df.filter(pl.col("RESTYPE") == "FAILURE")

print("\n4. FAILURE records:", failure.height)
print(
    "FAILURE records with CERT:",
    failure.filter(pl.col("CERT").is_not_null()).height
)

print(
    "Unique FAILURE CERT:",
    failure.filter(pl.col("CERT").is_not_null())
    .select(pl.col("CERT").n_unique())
    .item()
)

print("\n5. CERT data type:", df.schema["CERT"])

print("\n6. Distinct RESTYPE values:")
print(df.select("RESTYPE").unique().sort("RESTYPE"))

print("\nCHECK COMPLETED")
