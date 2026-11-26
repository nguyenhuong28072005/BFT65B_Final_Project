
import polars as pl
from pathlib import Path

# Load audit data
audit = pl.read_parquet(
    "data/interim/failure_event_audit.parquet"
)

# Load financial reporting dates
files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

financials = pl.concat(
    [
        pl.read_parquet(file).select([
            pl.col("CERT").cast(pl.Int64, strict=False),
            pl.col("REPDTE").cast(pl.String)
        ])
        for file in files
    ],
    how="vertical"
).with_columns(
    pl.col("REPDTE").str.strptime(
        pl.Date, format="%Y%m%d", strict=False
    ).alias("REPORT_DATE")
)

# Find the latest report date before each failure event
latest_reports = (
    audit
    .join(
        financials,
        on="CERT",
        how="inner"
    )
    .filter(
        pl.col("REPORT_DATE") < pl.col("FAILDATE_PARSED")
    )
    .group_by(["CERT", "ID", "FAILDATE_PARSED"])
    .agg(
        pl.col("REPORT_DATE").max().alias("LAST_REPORT_BEFORE_FAILURE")
    )
)

gap = (
    audit
    .join(
        latest_reports,
        on=["CERT", "ID", "FAILDATE_PARSED"],
        how="left"
    )
    .with_columns(
        (
            pl.col("FAILDATE_PARSED") -
            pl.col("LAST_REPORT_BEFORE_FAILURE")
        ).dt.total_days().alias("DAYS_TO_FAILURE")
    )
)

print("=" * 65)
print("FAILURE REPORTING GAP ANALYSIS")
print("=" * 65)

print("\nEvents with a prior financial report:")
print(gap["LAST_REPORT_BEFORE_FAILURE"].is_not_null().sum())

print("\nEvents without a prior financial report:")
print(gap["LAST_REPORT_BEFORE_FAILURE"].is_null().sum())

print("\nGap in days between last report and failure:")
print(
    gap.select("DAYS_TO_FAILURE").describe()
)

print("\nGap categories:")
gap = gap.with_columns(
    pl.when(pl.col("DAYS_TO_FAILURE").is_null())
    .then(pl.lit("NO_PRIOR_REPORT"))
    .when(pl.col("DAYS_TO_FAILURE") <= 365)
    .then(pl.lit("WITHIN_1_YEAR"))
    .when(pl.col("DAYS_TO_FAILURE") <= 730)
    .then(pl.lit("1_TO_2_YEARS"))
    .when(pl.col("DAYS_TO_FAILURE") <= 1460)
    .then(pl.lit("2_TO_4_YEARS"))
    .otherwise(pl.lit("OVER_4_YEARS"))
    .alias("GAP_CATEGORY")
)

print(
    gap.group_by("GAP_CATEGORY")
    .agg(pl.len().alias("EVENTS"))
    .sort("EVENTS", descending=True)
)

# Save detailed results
Path("data/interim").mkdir(parents=True, exist_ok=True)
gap.write_parquet(
    "data/interim/failure_reporting_gap.parquet"
)

print("\nSaved: data/interim/failure_reporting_gap.parquet")
print("\nANALYSIS COMPLETED")
