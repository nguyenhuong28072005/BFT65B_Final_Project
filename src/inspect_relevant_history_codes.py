import polars as pl

FILE = "data/interim/institution_history.parquet"

df = pl.read_parquet(FILE)

print("=" * 75)
print("1. FULL CHANGE CODE DISTRIBUTION")
print("=" * 75)

codes = (
    df.group_by(["CHANGECODE", "CHANGECODE_DESC"])
    .len()
    .sort("CHANGECODE")
)

print(codes)

# Các mã cần xem xét kỹ hơn
selected_codes = [110, 210, 211, 212, 213, 214, 215,
                  216, 217, 218, 219, 220, 221, 222,
                  223, 224, 225, 230, 235, 810]

relevant = df.filter(
    pl.col("CHANGECODE").is_in(selected_codes)
)

print("\n" + "=" * 75)
print("2. RELEVANT CHANGE CODE COUNTS")
print("=" * 75)

print(
    relevant.group_by(["CHANGECODE", "CHANGECODE_DESC"])
    .len()
    .sort("CHANGECODE")
)

print("\n" + "=" * 75)
print("3. IDENTIFIER AND DATE COMPLETENESS")
print("=" * 75)

print(
    relevant.select([
        pl.len().alias("TOTAL_RECORDS"),
        pl.col("CERT").null_count().alias("MISSING_CERT"),
        pl.col("EFFDATE").null_count().alias("MISSING_EFFDATE"),
        pl.col("ACQDATE").null_count().alias("MISSING_ACQDATE"),
        pl.col("PROCDATE").null_count().alias("MISSING_PROCDATE"),
        pl.col("ACQ_UNINUM").null_count().alias("MISSING_ACQ_UNINUM"),
        pl.col("OUT_UNINUM").null_count().alias("MISSING_OUT_UNINUM"),
    ])
)

print("\n" + "=" * 75)
print("4. SAMPLE DETAILS FOR EACH RELEVANT CODE")
print("=" * 75)

for code in selected_codes:
    subset = relevant.filter(
        pl.col("CHANGECODE") == code
    )

    if subset.height > 0:
        print(f"\n--- CODE {code} ---")
        print(
            subset.select([
                "CERT",
                "INSTNAME",
                "CHANGECODE_DESC",
                "EFFDATE",
                "ACQDATE",
                "PROCDATE",
                "ACQ_UNINUM",
                "OUT_UNINUM",
            ]).head(5)
        )

print("\nCHECK COMPLETED")