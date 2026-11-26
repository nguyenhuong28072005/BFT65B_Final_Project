
import polars as pl
from pathlib import Path

# Load failure events
failures = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
).with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date,
        format="%m/%d/%Y",
        strict=False
    ).alias("FAILDATE_PARSED")
)

# Load financial data
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
        pl.Date,
        format="%Y%m%d",
        strict=False
    ).alias("REPORT_DATE")
)

# Keep FAILURE events only
failure_events = failures.filter(
    (pl.col("RESTYPE") == "FAILURE") &
    pl.col("CERT").is_not_null()
)

# Get first and last financial reporting dates for each CERT
bank_periods = (
    financials
    .group_by("CERT")
    .agg([
        pl.col("REPORT_DATE").min().alias("FIRST_REPORT"),
        pl.col("REPORT_DATE").max().alias("LAST_REPORT"),
        pl.len().alias("FINANCIAL_RECORDS")
    ])
)

# Match events with financial reporting periods
audit = failure_events.join(
    bank_periods,
    on="CERT",
    how="left"
).with_columns(
    pl.when(pl.col("FIRST_REPORT").is_null())
    .then(pl.lit("NO_FINANCIAL_HISTORY"))
    .when(pl.col("FAILDATE_PARSED") < pl.col("FIRST_REPORT"))
    .then(pl.lit("BEFORE_FIRST_REPORT"))
    .when(pl.col("FAILDATE_PARSED") > pl.col("LAST_REPORT"))
    .then(pl.lit("AFTER_LAST_REPORT"))
    .otherwise(pl.lit("WITHIN_REPORTING_PERIOD"))
    .alias("EVENT_STATUS")
)

print("=" * 65)
print("FAILURE EVENT AUDIT")
print("=" * 65)

print("\nTotal FAILURE events with CERT:", audit.height)

print("\nEvent status:")
print(
    audit.group_by("EVENT_STATUS")
    .agg(pl.len().alias("EVENTS"))
    .sort("EVENTS", descending=True)
)

print("\nUnique banks by event status:")
print(
    audit.group_by("EVENT_STATUS")
    .agg(pl.col("CERT").n_unique().alias("UNIQUE_BANKS"))
)

print("\nMultiple FAILURE events by CERT:")
multiple = (
    audit.group_by("CERT")
    .agg([
        pl.len().alias("EVENT_COUNT"),
        pl.col("FAILDATE_PARSED").n_unique().alias("DISTINCT_DATES")
    ])
    .filter(pl.col("EVENT_COUNT") > 1)
    .sort("EVENT_COUNT", descending=True)
)

print(multiple)
print("\nNumber of CERT with multiple FAILURE events:", multiple.height)

# Save audit output
Path("data/interim").mkdir(parents=True, exist_ok=True)
audit.write_parquet("data/interim/failure_event_audit.parquet")

print("\nSaved: data/interim/failure_event_audit.parquet")
print("\nAUDIT COMPLETED")
