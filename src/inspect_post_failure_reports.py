
import polars as pl
from pathlib import Path

# ==================================================
# 1. LOAD FAILURE EVENTS
# ==================================================

failure_file = Path("data/raw/failures/failures_enriched.parquet")

failures = (
    pl.read_parquet(failure_file)
    .filter(
        (pl.col("RESTYPE") == "FAILURE")
        & pl.col("CERT").is_not_null()
    )
    .with_columns([
        pl.col("CERT").cast(pl.Int64),
        pl.col("FAILDATE")
        .cast(pl.String)
        .str.strptime(pl.Date, strict=False)
    ])
    .filter(pl.col("FAILDATE").is_not_null())
)

# Earliest recorded failure date for each CERT
first_failures = (
    failures
    .group_by("CERT")
    .agg(
        pl.col("FAILDATE").min().alias("FIRST_FAILURE_DATE"),
        pl.len().alias("FAILURE_EVENT_COUNT")
    )
)

# ==================================================
# 2. LOAD FINANCIAL REPORTING DATES
# ==================================================

financial_folder = Path("data/raw/financials_parts")
financial_files = sorted(
    financial_folder.glob("financials_offset_*.parquet")
)

financial_parts = []

for file in financial_files:
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
    financial_parts.append(part)

financials = pl.concat(
    financial_parts,
    how="vertical"
)

# ==================================================
# 3. GET REPORTING HISTORY AFTER FIRST FAILURE
# ==================================================

reporting_summary = (
    financials
    .group_by("CERT")
    .agg([
        pl.col("REPORT_DATE").min().alias("FIRST_REPORT_DATE"),
        pl.col("REPORT_DATE").max().alias("LAST_REPORT_DATE"),
        pl.len().alias("TOTAL_REPORTS")
    ])
)

result = (
    first_failures
    .join(reporting_summary, on="CERT", how="left")
    .with_columns(
        (
            pl.col("LAST_REPORT_DATE")
            > pl.col("FIRST_FAILURE_DATE")
        )
        .fill_null(False)
        .alias("HAS_REPORT_AFTER_FAILURE")
    )
)

# ==================================================
# 4. SUMMARY
# ==================================================

print("=" * 70)
print("FINANCIAL REPORTS AFTER FIRST FAILURE")
print("=" * 70)

print("\nFailure CERTs:", result.height)

print("\nReporting status:")
print(
    result.group_by("HAS_REPORT_AFTER_FAILURE")
    .len()
    .sort("HAS_REPORT_AFTER_FAILURE")
)

after_failure = (
    result
    .filter(pl.col("HAS_REPORT_AFTER_FAILURE"))
    .sort("FIRST_FAILURE_DATE")
)

print("\nCERTs with reports after first failure:", after_failure.height)

print("\nExamples:")
print(
    after_failure.select([
        "CERT",
        "FIRST_FAILURE_DATE",
        "LAST_REPORT_DATE",
        "TOTAL_REPORTS",
        "FAILURE_EVENT_COUNT"
    ]).head(20)
)

# ==================================================
# 5. SAVE RESULTS
# ==================================================

output_folder = Path("data/interim")
output_folder.mkdir(parents=True, exist_ok=True)

output_file = output_folder / "post_failure_reporting_audit.parquet"

result.write_parquet(output_file)

print("\nSaved:", output_file)
print("\nCHECK COMPLETED")
