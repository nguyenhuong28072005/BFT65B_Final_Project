
import polars as pl
from pathlib import Path

FILE_PATH = Path("data/raw/institutions/institutions.parquet")

df = pl.read_parquet(FILE_PATH)

print("=" * 60)
print("INSTITUTIONS DATA QUALITY CHECK")
print("=" * 60)

print("\nTotal records:", f"{df.height:,}")

print("\nData schema:")
print(df.schema)

print("\nMissing values by column:")

for column in df.columns:
    missing_count = df[column].null_count()
    missing_pct = missing_count / df.height * 100

    print(
        f"{column:12s}: {missing_count:>6,} "
        f"missing ({missing_pct:.2f}%)"
    )

print("\nDuplicate ID records:")
print(df.height - df["ID"].n_unique())

print("\nDuplicate CERT records:")
print(df.height - df["CERT"].n_unique())

print("\nACTIVE value distribution:")
print(
    df.group_by("ACTIVE")
    .len()
    .sort("ACTIVE")
)

print("\nCHANGEC1 value distribution:")
print(
    df.group_by("CHANGEC1")
    .len()
    .sort("len", descending=True)
    .head(10)
)

print("\nSample records:")
print(df.head(5))

print("\n" + "=" * 60)
print("INSTITUTIONS DATA QUALITY CHECK COMPLETED")
print("=" * 60)
