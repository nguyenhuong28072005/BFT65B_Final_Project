
import polars as pl
from pathlib import Path

# 1. Load all FAILURE events
failures = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
)

failures = (
    failures
    .filter(
        (pl.col("RESTYPE") == "FAILURE")
        & pl.col("CERT").is_not_null()
    )
    .with_columns([
        pl.col("CERT").cast(pl.Int64),
        pl.col("FAILDATE")
        .str.strptime(pl.Date, format="%m/%d/%Y", strict=False)
    ])
    .filter(pl.col("FAILDATE").is_not_null())
)

# 2. Get first failure date per CERT
first_failure = (
    failures
    .group_by("CERT")
    .agg(
        pl.col("FAILDATE").min().alias("FIRST_FAILURE_DATE")
    )
)

# 3. Load Financials
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

parts = []

for file in files:
    part = (
        pl.read_parquet(file)
        .select(["CERT", "REPDTE"])
        .with_columns([
            pl.col("CERT").cast(pl.Int64),
            pl.col("REPDTE")
            .cast(pl.String)
            .str.strptime(
                pl.Date,
                format="%Y%m%d",
                strict=False
            )
            .alias("REPORT_DATE")
        ])
        .select(["CERT", "REPORT_DATE"])
        .filter(
            pl.col("CERT").is_not_null()
            & pl.col("REPORT_DATE").is_not_null()
        )
    )
    parts.append(part)

financials = pl.concat(parts, how="vertical")

# 4. Identify observations before/at/after first failure
joined = financials.join(
    first_failure,
    on="CERT",
    how="left"
)

joined = joined.with_columns(
    pl.when(pl.col("FIRST_FAILURE_DATE").is_null())
    .then(pl.lit("NO_RECORDED_FAILURE"))
    .when(pl.col("REPORT_DATE") < pl.col("FIRST_FAILURE_DATE"))
    .then(pl.lit("BEFORE_FIRST_FAILURE"))
    .when(pl.col("REPORT_DATE") == pl.col("FIRST_FAILURE_DATE"))
    .then(pl.lit("ON_FIRST_FAILURE_DATE"))
    .otherwise(pl.lit("AFTER_FIRST_FAILURE"))
    .alias("OBSERVATION_STATUS")
)

print("=" * 70)
print("AT-RISK OBSERVATION AUDIT")
print("=" * 70)

print("\nAll financial observations:")
print(
    joined.group_by("OBSERVATION_STATUS")
    .len()
    .sort("OBSERVATION_STATUS")
)

print("\nUnique CERT by observation status:")
print(
    joined.group_by("OBSERVATION_STATUS")
    .agg(pl.col("CERT").n_unique().alias("UNIQUE_CERT"))
    .sort("OBSERVATION_STATUS")
)

# 5. Inspect the three repeated CERTs
repeated_certs = [29400, 31927, 32059]

print("\nThree repeated CERTs:")
print(
    joined
    .filter(pl.col("CERT").is_in(repeated_certs))
    .group_by(["CERT", "OBSERVATION_STATUS"])
    .len()
    .sort(["CERT", "OBSERVATION_STATUS"])
)

# 6. Save audit
output_folder = Path("data/interim")
output_folder.mkdir(parents=True, exist_ok=True)

output_file = output_folder / "at_risk_observation_audit.parquet"
joined.write_parquet(output_file)

print("\nSaved:", output_file)
print("CHECK COMPLETED")
