
import polars as pl

FILE = "data/interim/history_failure_date_comparison.parquet"

df = pl.read_parquet(FILE)

# Keep only History events occurring before or on Failure date
result = (
    df
    .filter(pl.col("SIGNED_GAP_DAYS") <= 0)
    .select([
        "CERT",
        "CHANGECODE",
        "CHANGECODE_DESC",
        "HISTORY_DATE",
        "FAILURE_DATE",
        "SIGNED_GAP_DAYS"
    ])
    .sort(["CHANGECODE", "CERT"])
)

pl.Config.set_tbl_rows(100)
pl.Config.set_tbl_cols(10)
pl.Config.set_fmt_str_lengths(60)

print("\n===== HISTORY EVENTS BEFORE OR ON FAILURE =====")
print("Total event pairs:", result.height)
print("Unique CERT:", result["CERT"].n_unique())

print(result)

result.write_csv(
    "reports/tables/pre_failure_history_events.csv"
)

print("\nSaved: reports/tables/pre_failure_history_events.csv")
