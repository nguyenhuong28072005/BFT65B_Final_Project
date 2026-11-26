
import polars as pl
from pathlib import Path

# 1. Load audit data
audit = pl.read_parquet(
    "data/interim/post_failure_reporting_audit.parquet"
)

# 2. Load financial reporting dates
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

# 3. Join failure date to reporting history
post_failure = (
    financials
    .join(
        audit.select(["CERT", "FIRST_FAILURE_DATE"]),
        on="CERT",
        how="inner"
    )
    .filter(
        pl.col("REPORT_DATE") > pl.col("FIRST_FAILURE_DATE")
    )
    .with_columns(
        (
            pl.col("REPORT_DATE") - pl.col("FIRST_FAILURE_DATE")
        ).dt.total_days().alias("DAYS_AFTER_FAILURE")
    )
)

# 4. Summarize each CERT
summary = (
    post_failure
    .group_by("CERT")
    .agg([
        pl.col("FIRST_FAILURE_DATE").first(),
        pl.col("REPORT_DATE").min().alias("FIRST_REPORT_AFTER_FAILURE"),
        pl.col("REPORT_DATE").max().alias("LAST_REPORT_AFTER_FAILURE"),
        pl.len().alias("REPORTS_AFTER_FAILURE"),
        pl.col("DAYS_AFTER_FAILURE").min().alias("DAYS_TO_FIRST_REPORT"),
        pl.col("DAYS_AFTER_FAILURE").max().alias("DAYS_TO_LAST_REPORT")
    ])
    .sort("DAYS_TO_LAST_REPORT", descending=True)
)

# 5. Timing categories
summary = summary.with_columns(
    pl.when(pl.col("DAYS_TO_LAST_REPORT") <= 120)
    .then(pl.lit("WITHIN_120_DAYS"))
    .when(pl.col("DAYS_TO_LAST_REPORT") <= 365)
    .then(pl.lit("121_TO_365_DAYS"))
    .when(pl.col("DAYS_TO_LAST_REPORT") <= 730)
    .then(pl.lit("1_TO_2_YEARS"))
    .otherwise(pl.lit("OVER_2_YEARS"))
    .alias("REPORTING_DURATION")
)

print("=" * 70)
print("POST-FAILURE REPORTING TIMING")
print("=" * 70)

print("\nNumber of CERTs with post-failure reports:", summary.height)

print("\nReporting duration:")
print(
    summary.group_by("REPORTING_DURATION")
    .len()
    .sort("REPORTING_DURATION")
)

print("\nSummary statistics (days):")
print(
    summary.select([
        pl.col("DAYS_TO_FIRST_REPORT").min().alias("Min"),
        pl.col("DAYS_TO_FIRST_REPORT").median().alias("Median"),
        pl.col("DAYS_TO_FIRST_REPORT").max().alias("Max"),
        pl.col("DAYS_TO_LAST_REPORT").median().alias("Median last report"),
        pl.col("DAYS_TO_LAST_REPORT").max().alias("Maximum last report")
    ])
)

print("\n20 CERTs with the longest post-failure reporting:")
print(
    summary.select([
        "CERT",
        "FIRST_FAILURE_DATE",
        "FIRST_REPORT_AFTER_FAILURE",
        "LAST_REPORT_AFTER_FAILURE",
        "REPORTS_AFTER_FAILURE",
        "DAYS_TO_FIRST_REPORT",
        "DAYS_TO_LAST_REPORT"
    ]).head(20)
)

# 6. Save
output = Path("data/interim/post_failure_timing_audit.parquet")
summary.write_parquet(output)

print("\nSaved:", output)
print("CHECK COMPLETED")
