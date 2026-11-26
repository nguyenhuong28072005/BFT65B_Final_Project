
import polars as pl
from pathlib import Path

DATA_DIR = Path("data/raw/financials_parts")
files = sorted(DATA_DIR.glob("financials_offset_*.parquet"))

target_certs = [29400, 31927, 32059]

# Standardize all columns to prevent schema conflicts
dataframes = []

for file in files:
    df = pl.read_parquet(file)

    df = df.with_columns(
        pl.col("CERT").cast(pl.Int64, strict=False),
        pl.col("REPDTE").cast(pl.String, strict=False),
        pl.col("ASSET").cast(pl.Float64, strict=False),
        pl.col("DEP").cast(pl.Float64, strict=False),
        pl.col("EQ").cast(pl.Float64, strict=False),
        pl.col("NETINC").cast(pl.Float64, strict=False),
        pl.col("ROA").cast(pl.Float64, strict=False),
        pl.col("NCLNLS").cast(pl.Float64, strict=False),
        pl.col("LNATRES").cast(pl.Float64, strict=False),
        pl.col("RBC1AAJ").cast(pl.Float64, strict=False),
        pl.col("ID").cast(pl.String, strict=False),
    )

    dataframes.append(df)

financials = pl.concat(dataframes, how="vertical")

# Read failure events
failures = pl.read_parquet(
    "data/raw/failures/failures_enriched.parquet"
).with_columns(
    pl.col("CERT").cast(pl.Int64, strict=False),
    pl.col("FAILDATE").str.strptime(
        pl.Date, format="%m/%d/%Y", strict=False
    ).alias("FAILDATE_PARSED")
)

# Inspect financial history around each event
for cert in target_certs:
    print("\n" + "=" * 65)
    print(f"CERT: {cert}")
    print("=" * 65)

    events = (
        failures
        .filter(
            (pl.col("CERT") == cert) &
            (pl.col("RESTYPE") == "FAILURE")
        )
        .sort("FAILDATE_PARSED")
    )

    bank_history = (
        financials
        .filter(pl.col("CERT") == cert)
        .with_columns(
            pl.col("REPDTE").str.strptime(
                pl.Date, format="%Y%m%d", strict=False
            ).alias("REPORT_DATE")
        )
    )

    for event in events.iter_rows(named=True):
        event_date = event["FAILDATE_PARSED"]

        print(
            f"\nEvent: {event_date} | "
            f"FIN: {event['FIN']} | "
            f"Name: {event['NAME']}"
        )

        history = (
            bank_history
            .filter(
                (pl.col("REPORT_DATE") >=
                 pl.lit(event_date).dt.offset_by("-8y")) &
                (pl.col("REPORT_DATE") <=
                 pl.lit(event_date).dt.offset_by("1y"))
            )
            .sort("REPORT_DATE")
            .select([
                "REPORT_DATE",
                "ASSET",
                "DEP",
                "EQ",
                "NETINC",
                "ROA"
            ])
        )

        print(history)
        print("Number of financial observations:", history.height)

print("\n" + "=" * 65)
print("INVESTIGATION COMPLETED")
print("=" * 65)
