
import polars as pl
from pathlib import Path

# 1. Load first failure date per CERT
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

first_failure = (
    failures
    .group_by("CERT")
    .agg(
        pl.col("FAILDATE").min().alias("FIRST_FAILURE_DATE")
    )
)

# 2. Load Financials
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

# 3. Join first failure date
joined = financials.join(
    first_failure,
    on="CERT",
    how="left"
)

# 4. Apply modeling cutoff and at-risk rule
cutoff = pl.date(2025, 6, 30)

eligible = joined.filter(
    (pl.col("REPORT_DATE") <= cutoff)
    & (
        pl.col("FIRST_FAILURE_DATE").is_null()
        | (pl.col("REPORT_DATE") < pl.col("FIRST_FAILURE_DATE"))
    )
)

# 5. Summary
print("=" * 70)
print("MODELING SAMPLE AUDIT")
print("=" * 70)

print("\nOriginal Financials observations:", financials.height)
print("Observations on/before 2025-06-30:",
      joined.filter(pl.col("REPORT_DATE") <= cutoff).height)

print("\nEligible observations:", eligible.height)
print("Unique eligible CERT:", eligible["CERT"].n_unique())

print("\nEligible observations by failure history:")
print(
    eligible.with_columns(
        pl.when(pl.col("FIRST_FAILURE_DATE").is_null())
        .then(pl.lit("NO_RECORDED_FAILURE"))
        .otherwise(pl.lit("HAS_FUTURE_OR_PAST_FAILURE"))
        .alias("FAILURE_GROUP")
    )
    .group_by("FAILURE_GROUP")
    .agg([
        pl.len().alias("OBSERVATIONS"),
        pl.col("CERT").n_unique().alias("UNIQUE_CERT")
    ])
)

print("\nObservations by year:")
print(
    eligible.with_columns(
        pl.col("REPORT_DATE").dt.year().alias("YEAR")
    )
    .group_by("YEAR")
    .len()
    .sort("YEAR")
)

# 6. Save
output_folder = Path("data/interim")
output_folder.mkdir(parents=True, exist_ok=True)

output_file = output_folder / "eligible_modeling_observations.parquet"
eligible.write_parquet(output_file)

print("\nSaved:", output_file)
print("CHECK COMPLETED")
