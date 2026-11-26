
import polars as pl

# 1. Load previous audit results
audit = pl.read_parquet(
    "data/interim/bank_exit_audit.parquet"
)

# 2. Load Institutions data
institutions = pl.read_parquet(
    "data/raw/institutions/institutions.parquet"
).with_columns([
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("ACTIVE").cast(pl.Int64, strict=False),
    pl.col("CHANGEC1").cast(pl.Int64, strict=False)
])

# 3. Select potential exits
potential_exits = audit.filter(
    (pl.col("EVENT_STATUS") == "NO_RECORDED_EVENT")
    & (pl.col("LAST_REPORT_DATE") < pl.date(2025, 1, 1))
)

# 4. Join with Institutions
comparison = potential_exits.join(
    institutions.select(["CERT", "STNAME", "ACTIVE", "CHANGEC1"]),
    on="CERT",
    how="left"
)

print("=" * 70)
print("POTENTIAL EXITS - INSTITUTIONS CROSS-CHECK")
print("=" * 70)

print("Potential exit banks:", potential_exits.height)
print(
    "Matched with Institutions:",
    comparison.filter(pl.col("STNAME").is_not_null()).height
)
print(
    "Not matched with Institutions:",
    comparison.filter(pl.col("STNAME").is_null()).height
)

# 5. ACTIVE distribution
print("\n" + "=" * 70)
print("1. ACTIVE DISTRIBUTION")
print("=" * 70)

print(
    comparison
    .group_by("ACTIVE")
    .agg(pl.len().alias("NUMBER_OF_BANKS"))
    .sort("ACTIVE")
)

# 6. CHANGEC1 distribution
print("\n" + "=" * 70)
print("2. CHANGEC1 DISTRIBUTION")
print("=" * 70)

print(
    comparison
    .group_by("CHANGEC1")
    .agg(pl.len().alias("NUMBER_OF_BANKS"))
    .sort("NUMBER_OF_BANKS", descending=True)
)

# 7. Cross-tabulation
print("\n" + "=" * 70)
print("3. ACTIVE BY CHANGEC1")
print("=" * 70)

print(
    comparison
    .group_by(["ACTIVE", "CHANGEC1"])
    .agg(pl.len().alias("NUMBER_OF_BANKS"))
    .sort(["ACTIVE", "CHANGEC1"])
)

# 8. Save detailed result
comparison.write_parquet(
    "data/interim/potential_exits_institutions.parquet"
)

print("\nSaved: data/interim/potential_exits_institutions.parquet")
print("CHECK COMPLETED")
