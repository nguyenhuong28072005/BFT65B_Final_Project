
import polars as pl
from pathlib import Path

print("=" * 65)
print("INVESTIGATION OF UNMATCHED FINANCIALS CERT")
print("=" * 65)

files = sorted(
    Path("data/raw/financials_parts").glob("financials_offset_*.parquet")
)

financial_dfs = []

for file in files:
    df = pl.read_parquet(file)

    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False),
        pl.col("REPDTE").cast(pl.String, strict=False)
    )

    financial_dfs.append(df.select(["CERT", "REPDTE"]))

financials = pl.concat(financial_dfs, how="vertical")

institutions = pl.read_parquet(
    "data/raw/institutions/institutions.parquet"
).with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False)
)

financial_certs = financials.select("CERT").drop_nulls().unique()
institution_certs = institutions.select("CERT").drop_nulls().unique()

unmatched = financial_certs.join(
    institution_certs,
    on="CERT",
    how="anti"
)

unmatched_detail = financials.join(
    unmatched,
    on="CERT",
    how="inner"
)

summary = unmatched_detail.group_by("CERT").agg(
    pl.len().alias("Financial_records"),
    pl.col("REPDTE").min().alias("First_report_date"),
    pl.col("REPDTE").max().alias("Last_report_date")
).sort("Financial_records", descending=True)

print("\nNumber of unmatched CERT:", unmatched.height)
print("\nTotal financial records belonging to unmatched CERT:",
      unmatched_detail.height)

print("\nSummary of unmatched banks:")
print(summary.head(20))

print("\n" + "=" * 65)
print("INVESTIGATION COMPLETED")
print("=" * 65)
