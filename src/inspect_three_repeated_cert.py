
import polars as pl

failures = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
)

certs = [29400, 31927, 32059]

print("=" * 70)
print("THREE CERTs WITH MULTIPLE FAILURE EVENTS")
print("=" * 70)

for cert in certs:
    print(f"\n{'=' * 50}")
    print(f"CERT: {cert}")
    print("=" * 50)

    events = (
        failures
        .filter(
            (pl.col("CERT") == cert)
            & (pl.col("RESTYPE") == "FAILURE")
        )
        .select([
            "CERT",
            "NAME",
            "CITYST",
            "FAILDATE",
            "FIN",
            "RESTYPE1",
            "COST",
            "ID"
        ])
        .sort("FAILDATE")
    )

    print("\nFailure events:")
    print(events)

print("\nCHECK COMPLETED")
