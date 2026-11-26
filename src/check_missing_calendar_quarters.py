
import polars as pl
import calendar
from datetime import date
from pathlib import Path

DATA_DIR = Path("data/raw/financials_parts")
files = sorted(DATA_DIR.glob("financials_offset_*.parquet"))

# Load unique reporting dates
financials = pl.concat(
    [
        pl.read_parquet(file).select(
            pl.col("REPDTE").cast(pl.String)
        )
        for file in files
    ],
    how="vertical"
).with_columns(
    pl.col("REPDTE").str.strptime(
        pl.Date, format="%Y%m%d", strict=False
    ).alias("REPORT_DATE")
)

actual_dates = set(
    financials
    .select("REPORT_DATE")
    .unique()
    .to_series()
    .to_list()
)

# Generate all expected quarter-end dates
expected_dates = []

for year in range(1984, 2027):
    for month in [3, 6, 9, 12]:
        if year == 2026 and month > 6:
            continue

        last_day = calendar.monthrange(year, month)[1]
        expected_dates.append(date(year, month, last_day))

missing_dates = sorted(set(expected_dates) - actual_dates)
unexpected_dates = sorted(actual_dates - set(expected_dates))

print("=" * 65)
print("CALENDAR QUARTER COMPLETENESS CHECK")
print("=" * 65)

print("\nExpected quarter dates:", len(expected_dates))
print("Actual reporting dates:", len(actual_dates))
print("Missing quarter dates:", len(missing_dates))

print("\nMissing dates:")
for d in missing_dates:
    print(d)

print("\nUnexpected reporting dates:")
for d in unexpected_dates:
    print(d)

print("\nCHECK COMPLETED")
