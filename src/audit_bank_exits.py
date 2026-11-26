
import polars as pl
from pathlib import Path

# ============================================================
# 1. LOAD FINANCIALS DATA
# ============================================================

print("=" * 70)
print("BANK EXIT AUDIT")
print("=" * 70)

financial_files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

if not financial_files:
    raise FileNotFoundError("No Financials Parquet files found.")

financial_parts = []

for file in financial_files:
    part = (
        pl.read_parquet(file)
        .select(["CERT", "REPDTE"])
        .with_columns([
            pl.col("CERT").cast(pl.Int64, strict=False),
            pl.col("REPDTE")
            .cast(pl.String)
            .str.strptime(pl.Date, format="%Y%m%d", strict=False)
            .alias("REPORT_DATE")
        ])
        .select(["CERT", "REPORT_DATE"])
        .filter(
            pl.col("CERT").is_not_null()
            & pl.col("REPORT_DATE").is_not_null()
        )
    )

    financial_parts.append(part)

financials = pl.concat(financial_parts, how="vertical")

# One row per bank: first and last reporting dates
bank_history = (
    financials
    .group_by("CERT")
    .agg([
        pl.col("REPORT_DATE").min().alias("FIRST_REPORT_DATE"),
        pl.col("REPORT_DATE").max().alias("LAST_REPORT_DATE"),
        pl.len().alias("NUMBER_OF_REPORTS")
    ])
)

global_last_date = financials["REPORT_DATE"].max()

print("\nFINANCIALS SUMMARY")
print("Total financial observations:", financials.height)
print("Unique banks:", bank_history.height)
print("First reporting date:", financials["REPORT_DATE"].min())
print("Last reporting date:", global_last_date)


# ============================================================
# 2. LOAD FAILURE AND ASSISTANCE EVENTS
# ============================================================

events = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
)

events = (
    events
    .with_columns([
        pl.col("CERT").cast(pl.Int64, strict=False),
        pl.col("FAILDATE")
        .cast(pl.String)
        .str.strptime(pl.Date, format="%m/%d/%Y", strict=False)
        .alias("EVENT_DATE")
    ])
    .filter(
        pl.col("CERT").is_not_null()
        & pl.col("EVENT_DATE").is_not_null()
    )
)

# Separate FAILURE and ASSISTANCE records
failure_summary = (
    events
    .filter(pl.col("RESTYPE") == "FAILURE")
    .group_by("CERT")
    .agg([
        pl.col("EVENT_DATE").min().alias("FIRST_FAILURE_DATE"),
        pl.len().alias("FAILURE_EVENT_COUNT")
    ])
)

assistance_summary = (
    events
    .filter(pl.col("RESTYPE") == "ASSISTANCE")
    .group_by("CERT")
    .agg([
        pl.col("EVENT_DATE").min().alias("FIRST_ASSISTANCE_DATE"),
        pl.len().alias("ASSISTANCE_EVENT_COUNT")
    ])
)


# ============================================================
# 3. COMBINE BANK HISTORY AND EVENT RECORDS
# ============================================================

audit = (
    bank_history
    .join(failure_summary, on="CERT", how="left")
    .join(assistance_summary, on="CERT", how="left")
    .with_columns([
        pl.col("FAILURE_EVENT_COUNT").fill_null(0),
        pl.col("ASSISTANCE_EVENT_COUNT").fill_null(0)
    ])
    .with_columns([
        pl.when(pl.col("FAILURE_EVENT_COUNT") > 0)
        .then(pl.lit("HAS_FAILURE_RECORD"))
        .when(pl.col("ASSISTANCE_EVENT_COUNT") > 0)
        .then(pl.lit("ASSISTANCE_ONLY"))
        .otherwise(pl.lit("NO_RECORDED_EVENT"))
        .alias("EVENT_STATUS"),

        (pl.lit(global_last_date) - pl.col("LAST_REPORT_DATE"))
        .dt.total_days()
        .alias("DAYS_SINCE_LAST_REPORT")
    ])
    .with_columns([
        pl.when(pl.col("LAST_REPORT_DATE") == pl.lit(global_last_date))
        .then(pl.lit("REPORTED_IN_LAST_QUARTER"))
        .when(pl.col("LAST_REPORT_DATE") >= pl.date(2025, 1, 1))
        .then(pl.lit("LAST_REPORTED_2025_OR_2026"))
        .when(pl.col("LAST_REPORT_DATE") >= pl.date(2020, 1, 1))
        .then(pl.lit("LAST_REPORTED_2020_TO_2024"))
        .otherwise(pl.lit("LAST_REPORTED_BEFORE_2020"))
        .alias("REPORTING_STATUS")
    ])
    .sort("LAST_REPORT_DATE")
)


# ============================================================
# 4. SUMMARY TABLES
# ============================================================

print("\n" + "=" * 70)
print("1. EVENT STATUS")
print("=" * 70)

print(
    audit
    .group_by("EVENT_STATUS")
    .agg(pl.len().alias("NUMBER_OF_BANKS"))
    .sort("EVENT_STATUS")
)

print("\n" + "=" * 70)
print("2. LAST REPORTING PERIOD")
print("=" * 70)

print(
    audit
    .group_by("REPORTING_STATUS")
    .agg(pl.len().alias("NUMBER_OF_BANKS"))
    .sort("REPORTING_STATUS")
)

print("\n" + "=" * 70)
print("3. EVENT STATUS BY REPORTING STATUS")
print("=" * 70)

print(
    audit
    .group_by(["REPORTING_STATUS", "EVENT_STATUS"])
    .agg(pl.len().alias("NUMBER_OF_BANKS"))
    .sort(["REPORTING_STATUS", "EVENT_STATUS"])
)


# ============================================================
# 5. IDENTIFY BANKS WITH OLD REPORTING RECORDS
# ============================================================

print("\n" + "=" * 70)
print("4. BANKS WITH NO RECORDED FAILURE/ASSISTANCE")
print("   AND LAST REPORT BEFORE 2025")
print("=" * 70)

potential_exits = (
    audit
    .filter(
        (pl.col("EVENT_STATUS") == "NO_RECORDED_EVENT")
        & (pl.col("LAST_REPORT_DATE") < pl.date(2025, 1, 1))
    )
    .sort("LAST_REPORT_DATE", descending=True)
)

print("Number of potential cases:", potential_exits.height)

print(
    potential_exits
    .select([
        "CERT",
        "FIRST_REPORT_DATE",
        "LAST_REPORT_DATE",
        "NUMBER_OF_REPORTS",
        "DAYS_SINCE_LAST_REPORT",
        "EVENT_STATUS"
    ])
    .head(30)
)


# ============================================================
# 6. SAVE RESULTS
# ============================================================

output_folder = Path("data/interim")
output_folder.mkdir(parents=True, exist_ok=True)

audit.write_parquet(
    output_folder / "bank_exit_audit.parquet"
)

(
    audit
    .group_by(["REPORTING_STATUS", "EVENT_STATUS"])
    .agg(pl.len().alias("NUMBER_OF_BANKS"))
    .sort(["REPORTING_STATUS", "EVENT_STATUS"])
    .write_csv(output_folder / "bank_exit_summary.csv")
)

potential_exits.write_parquet(
    output_folder / "potential_bank_exits.parquet"
)

print("\n" + "=" * 70)
print("AUDIT COMPLETED")
print("Saved:")
print("- data/interim/bank_exit_audit.parquet")
print("- data/interim/bank_exit_summary.csv")
print("- data/interim/potential_bank_exits.parquet")
print("=" * 70)
