
import polars as pl
from pathlib import Path

print("=" * 65)
print("DETAILED MISSING QUARTER INSPECTION")
print("=" * 65)

# Load Financials
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

dataframes = []

for file in files:
    df = pl.read_parquet(file)

    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False),
        pl.col("REPDTE").cast(pl.String, strict=False)
    )

    dataframes.append(df.select(["CERT", "REPDTE"]))

financials = pl.concat(dataframes, how="vertical")

# Create quarter index
financials = financials.with_columns(
    pl.col("REPDTE").str.strptime(
        pl.Date, format="%Y%m%d", strict=False
    ).alias("REPORT_DATE")
).with_columns(
    (
        pl.col("REPORT_DATE").dt.year() * 4
        + pl.col("REPORT_DATE").dt.quarter()
    ).alias("QUARTER_INDEX")
)

# Banks to inspect
target_certs = [27120, 22399, 34575, 25899, 16340]

for cert in target_certs:
    bank = financials.filter(pl.col("CERT") == cert)

    if bank.is_empty():
        continue

    observed = set(bank["QUARTER_INDEX"].to_list())

    first_q = min(observed)
    last_q = max(observed)

    expected = set(range(first_q, last_q + 1))
    missing = sorted(expected - observed)

    print(f"\nCERT: {cert}")
    print("Observed quarters:", len(observed))
    print("Missing quarters:", len(missing))

    if missing:
        missing_df = pl.DataFrame(
            {"QUARTER_INDEX": missing}
        ).with_columns(
            ((pl.col("QUARTER_INDEX") - 1) // 4).alias("YEAR"),
            (((pl.col("QUARTER_INDEX") - 1) % 4) + 1).alias("QUARTER")
        )

        print(missing_df)

print("\n" + "=" * 65)
print("DETAILED INSPECTION COMPLETED")
print("=" * 65)
