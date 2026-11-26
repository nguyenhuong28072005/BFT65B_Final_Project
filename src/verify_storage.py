
from pathlib import Path
import polars as pl

# 1. Define project paths
PROJECT_DIR = Path(__file__).resolve().parents[1]

INPUT_DIR = PROJECT_DIR / "data" / "raw" / "financials_parts"
OUTPUT_DIR = PROJECT_DIR / "benchmarks" / "storage_test"

CSV_PATH = OUTPUT_DIR / "financials_benchmark.csv"
PARQUET_PATH = OUTPUT_DIR / "financials_benchmark.parquet"

# 2. Read original Parquet batches
files = sorted(INPUT_DIR.glob("*.parquet"))

if not files:
    raise FileNotFoundError("Original Parquet batches not found.")

original_parts = [pl.read_parquet(file) for file in files]
original = pl.concat(original_parts, how="vertical_relaxed")

# 3. Read converted CSV and Parquet
csv_data = pl.read_csv(
    CSV_PATH,
    schema_overrides=original.schema
)

parquet_data = pl.read_parquet(PARQUET_PATH)

# 4. Check dimensions
print("ORIGINAL:", original.shape)
print("CSV:     ", csv_data.shape)
print("PARQUET: ", parquet_data.shape)

# 5. Compare complete data
csv_matches = original.equals(csv_data, null_equal=True)
parquet_matches = original.equals(parquet_data, null_equal=True)

print("\n========== DATA INTEGRITY CHECK ==========")
print("Original vs CSV:", csv_matches)
print("Original vs Parquet:", parquet_matches)

if csv_matches and parquet_matches:
    print("\nSUCCESS: Both formats preserve the original data.")
else:
    print("\nWARNING: A difference was detected. Further inspection is needed.")
