
import polars as pl
from pathlib import Path

# =====================================================
# 1. FILE PATHS
# =====================================================

HISTORY_FILE = Path("data/interim/institution_history.parquet")
FAILURES_FILE = Path("data/raw/failures/failures_enriched.parquet")

OUTPUT_FILE = Path("reports/tables/history_failure_date_comparison.csv")

# Các Change Code liên quan đến sự kiện đóng cửa,
# sáp nhập hoặc thay đổi tình trạng tổ chức
SELECTED_CODES = [211, 213, 215, 216, 223, 224, 230, 235]

# =====================================================
# 2. CHECK INPUT FILES
# =====================================================

if not HISTORY_FILE.exists():
    raise FileNotFoundError(f"Không tìm thấy: {HISTORY_FILE}")

if not FAILURES_FILE.exists():
    raise FileNotFoundError(f"Không tìm thấy: {FAILURES_FILE}")

history = pl.read_parquet(HISTORY_FILE)
failures = pl.read_parquet(FAILURES_FILE)

print("=" * 70)
print("1. INPUT DATA")
print("=" * 70)

print(f"History rows: {history.height:,}")
print(f"Failures rows: {failures.height:,}")
print(f"EFFDATE original dtype: {history.schema['EFFDATE']}")
print(f"FAILDATE original dtype: {failures.schema['FAILDATE']}")

# =====================================================
# 3. PARSE DATES USING EXPLICIT FORMATS
# =====================================================

# History:
# Ví dụ: 1986-02-06T00:00:00
# Lấy 10 ký tự đầu: 1986-02-06
# Định dạng: YYYY-MM-DD

history = history.with_columns([
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("EFFDATE")
    .cast(pl.String)
    .str.slice(0, 10)
    .str.strptime(
        pl.Date,
        format="%Y-%m-%d",
        strict=False
    )
    .alias("EFFDATE")
])

# Failures:
# Ví dụ: 2/6/1986
# Định dạng: MM/DD/YYYY

failures = failures.with_columns([
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE")
    .cast(pl.String)
    .str.strptime(
        pl.Date,
        format="%m/%d/%Y",
        strict=False
    )
    .alias("FAILDATE")
])

# =====================================================
# 4. CHECK PARSING QUALITY
# =====================================================

print("\n" + "=" * 70)
print("2. DATE PARSING QUALITY")
print("=" * 70)

history_invalid = history.filter(
    pl.col("EFFDATE").is_null()
).height

failures_invalid = failures.filter(
    pl.col("FAILDATE").is_null()
).height

print(f"History rows with null EFFDATE after parsing: {history_invalid:,}")
print(f"Failures rows with null FAILDATE after parsing: {failures_invalid:,}")

# =====================================================
# 5. SELECT HISTORY EVENTS
# =====================================================

h_events = (
    history
    .filter(
        pl.col("CHANGECODE").is_in(SELECTED_CODES)
        & pl.col("CERT").is_not_null()
        & pl.col("EFFDATE").is_not_null()
    )
    .select([
        "CERT",
        pl.col("EFFDATE").alias("HISTORY_DATE"),
        "CHANGECODE",
        "CHANGECODE_DESC"
    ])
    .unique()
)

# =====================================================
# 6. SELECT FAILURE EVENTS
# =====================================================

f_events = (
    failures
    .filter(
        (pl.col("RESTYPE") == "FAILURE")
        & pl.col("CERT").is_not_null()
        & pl.col("FAILDATE").is_not_null()
    )
    .select([
        "CERT",
        pl.col("FAILDATE").alias("FAILURE_DATE")
    ])
    .unique()
)

# =====================================================
# 7. MATCH HISTORY WITH FAILURE EVENTS
# =====================================================

comparison = (
    h_events
    .join(f_events, on="CERT", how="inner")
    .with_columns(
        (
            pl.col("HISTORY_DATE") - pl.col("FAILURE_DATE")
        ).dt.total_days().alias("SIGNED_GAP_DAYS")
    )
    .with_columns(
        pl.when(pl.col("SIGNED_GAP_DAYS") < 0)
        .then(pl.lit("HISTORY_BEFORE_FAILURE"))
        .when(pl.col("SIGNED_GAP_DAYS") > 0)
        .then(pl.lit("HISTORY_AFTER_FAILURE"))
        .otherwise(pl.lit("SAME_DATE"))
        .alias("DATE_RELATION")
    )
    .sort(["CHANGECODE", "CERT", "SIGNED_GAP_DAYS"])
)

# =====================================================
# 8. SAVE RESULTS
# =====================================================

OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

comparison.write_csv(OUTPUT_FILE)

# =====================================================
# 9. SUMMARY
# =====================================================

print("\n" + "=" * 70)
print("3. MATCHING SUMMARY")
print("=" * 70)

print(f"Selected history events: {h_events.height:,}")
print(f"Selected failure events: {f_events.height:,}")
print(f"Matched history-failure rows: {comparison.height:,}")

print("\nDate relation counts:")

print(
    comparison
    .group_by("DATE_RELATION")
    .agg(pl.len().alias("COUNT"))
    .sort("DATE_RELATION")
)

print("\nChange code summary:")

print(
    comparison
    .group_by(["CHANGECODE", "CHANGECODE_DESC", "DATE_RELATION"])
    .agg(pl.len().alias("COUNT"))
    .sort(["CHANGECODE", "DATE_RELATION"])
)

print("\n" + "=" * 70)
print("4. EXAMPLE MATCHED EVENTS")
print("=" * 70)

print(comparison.head(20))

print("\n" + "=" * 70)
print("COMPLETED")
print("=" * 70)

print(f"Output saved to: {OUTPUT_FILE}")

# =====================================================
# 10. INVESTIGATE CLOSING EVENTS BEFORE FAILURE DATE
# =====================================================

print("\n" + "=" * 70)
print("5. CLOSING EVENTS BEFORE FAILURE DATE")
print("=" * 70)

closing_before_failure = (
    comparison
    .filter(
        (pl.col("CHANGECODE") == 230)
        & (pl.col("SIGNED_GAP_DAYS") < 0)
    )
    .sort("SIGNED_GAP_DAYS")
)

print(f"Number of closing events before failure: {closing_before_failure.height}")

print(closing_before_failure)

print("\n" + "=" * 70)
print("6. CHECK FOUR PREVIOUSLY INVESTIGATED CERTS")
print("=" * 70)

target_cert_check = (
    comparison
    .filter(
        pl.col("CERT").is_in([4422, 9391, 11451, 22314])
    )
    .sort(["CERT", "CHANGECODE"])
)

print(target_cert_check)

# =====================================================
# 11. INSPECT ORIGINAL SOURCE RECORDS FOR 7 CERTS
# =====================================================

print("\n" + "=" * 70)
print("7. ORIGINAL SOURCE RECORDS FOR THE 7 CLOSING EVENTS")
print("=" * 70)

suspect_certs = [31889, 31959, 25193, 31155, 31323, 31674, 32314]

print("\n--- ORIGINAL HISTORY RECORDS ---")

history_source_check = (
    pl.read_parquet(HISTORY_FILE)
    .filter(
        pl.col("CERT").cast(pl.Int64, strict=False).is_in(suspect_certs)
        & (pl.col("CHANGECODE") == 230)
    )
    .select(["CERT", "EFFDATE", "CHANGECODE", "CHANGECODE_DESC"])
    .sort(["CERT", "EFFDATE"])
)

print(history_source_check)

print("\n--- ORIGINAL FAILURE RECORDS ---")

failure_source_check = (
    pl.read_parquet(FAILURES_FILE)
    .filter(
        pl.col("CERT").cast(pl.Int64, strict=False).is_in(suspect_certs)
        & (pl.col("RESTYPE") == "FAILURE")
    )
    .select(["CERT", "FAILDATE", "RESTYPE"])
    .sort(["CERT", "FAILDATE"])
)

print(failure_source_check)

# =====================================================
# 12. CHECK MULTIPLE FAILURE RECORDS FOR SUSPECT CERTS
# =====================================================

print("\n" + "=" * 70)
print("8. CHECK NUMBER OF FAILURE RECORDS PER SUSPECT CERT")
print("=" * 70)

failure_count_check = (
    pl.read_parquet(FAILURES_FILE)
    .filter(
        pl.col("CERT").cast(pl.Int64, strict=False).is_in(suspect_certs)
        & (pl.col("RESTYPE") == "FAILURE")
    )
    .group_by("CERT")
    .agg([
        pl.len().alias("FAILURE_RECORD_COUNT"),
        pl.col("FAILDATE").alias("ALL_FAILURE_DATES")
    ])
    .sort("CERT")
)

print(failure_count_check)
