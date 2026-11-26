
import polars as pl
from pathlib import Path

failures = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
)

failures = failures.with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

target_certs = [29400, 31927, 32059]

print("=" * 65)
print("REPEATED FAILURE EVENTS: FIN INVESTIGATION")
print("=" * 65)

for cert in target_certs:
    print(f"\nCERT: {cert}")

    records = (
        failures
        .filter(
            (pl.col("CERT") == cert) &
            (pl.col("RESTYPE") == "FAILURE")
        )
        .sort("FAILDATE_PARSED")
        .select([
            "CERT",
            "FIN",
            "NAME",
            "FAILDATE_PARSED",
            "RESTYPE",
            "RESTYPE1",
            "ID"
        ])
    )

    print(records)

    print("Distinct FIN:", records["FIN"].n_unique())
    print("Distinct names:", records["NAME"].n_unique())

print("\n" + "=" * 65)
print("INVESTIGATION COMPLETED")
print("=" * 65)
