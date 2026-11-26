
import polars as pl
from pathlib import Path

print("=" * 65)
print("FDIC DATASET RELATIONSHIP CHECK")
print("=" * 65)

# 1. Load and standardize Financials files
financial_files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

financial_dfs = []

for file in financial_files:
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

    financial_dfs.append(df)

financials = pl.concat(financial_dfs, how="vertical")

# 2. Load Failures and Institutions
failures = pl.read_parquet(
    "data/raw/failures/failures.parquet"
)

institutions = pl.read_parquet(
    "data/raw/institutions/institutions.parquet"
)

# 3. Standardize CERT data types
failures = failures.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False)
)

institutions = institutions.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False)
)

# 4. Extract unique CERT values
financial_certs = (
    financials.select("CERT").drop_nulls().unique()
)

failure_certs = (
    failures.select("CERT").drop_nulls().unique()
)

institution_certs = (
    institutions.select("CERT").drop_nulls().unique()
)

# 5. Financials vs Institutions
financial_match = financial_certs.join(
    institution_certs,
    on="CERT",
    how="inner"
)

financial_unmatched = financial_certs.join(
    institution_certs,
    on="CERT",
    how="anti"
)

# 6. Failures vs Institutions
failure_match = failure_certs.join(
    institution_certs,
    on="CERT",
    how="inner"
)

failure_unmatched = failure_certs.join(
    institution_certs,
    on="CERT",
    how="anti"
)

# 7. Print results
print("\n1. FINANCIALS vs INSTITUTIONS")
print("Unique Financials CERT:", f"{financial_certs.height:,}")
print("Matched CERT:", f"{financial_match.height:,}")
print("Unmatched CERT:", f"{financial_unmatched.height:,}")

print("\n2. FAILURES vs INSTITUTIONS")
print("Unique Failures CERT:", f"{failure_certs.height:,}")
print("Matched CERT:", f"{failure_match.height:,}")
print("Unmatched CERT:", f"{failure_unmatched.height:,}")

print("\n3. MISSING CERT RECORDS")
print("Failures with missing CERT:", failures["CERT"].null_count())

print("\n4. SAMPLE UNMATCHED FINANCIALS CERT")
print(financial_unmatched.head(10))

print("\n5. SAMPLE UNMATCHED FAILURES CERT")
print(failure_unmatched.head(10))

print("\n" + "=" * 65)
print("RELATIONSHIP CHECK COMPLETED")
print("=" * 65)
