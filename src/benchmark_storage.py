
from pathlib import Path
import time

import polars as pl


# 1. Define input and output folders
PROJECT_DIR = Path(__file__).resolve().parents[1]

INPUT_DIR = PROJECT_DIR / "data" / "raw" / "financials_parts"
OUTPUT_DIR = PROJECT_DIR / "benchmarks" / "storage_test"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

CSV_PATH = OUTPUT_DIR / "financials_benchmark.csv"
PARQUET_PATH = OUTPUT_DIR / "financials_benchmark.parquet"


# 2. Find all downloaded Parquet batches
parquet_files = sorted(INPUT_DIR.glob("*.parquet"))

if not parquet_files:
    raise FileNotFoundError(
        f"No Parquet files found in: {INPUT_DIR}"
    )

print(f"Number of input files: {len(parquet_files)}")


# 3. Read and combine all batches
start_time = time.perf_counter()

dataframes = [pl.read_parquet(file) for file in parquet_files]
df = pl.concat(dataframes, how="vertical_relaxed")

load_time = time.perf_counter() - start_time

print(f"Total records: {df.height:,}")
print(f"Number of columns: {df.width}")
print(f"Initial loading time: {load_time:.2f} seconds")


# 4. Write CSV and measure writing time
start_time = time.perf_counter()

df.write_csv(CSV_PATH)

csv_write_time = time.perf_counter() - start_time


# 5. Write compressed Parquet and measure writing time
start_time = time.perf_counter()

df.write_parquet(
    PARQUET_PATH,
    compression="zstd"
)

parquet_write_time = time.perf_counter() - start_time


# 6. Measure file sizes
csv_size_mb = CSV_PATH.stat().st_size / (1024 ** 2)
parquet_size_mb = PARQUET_PATH.stat().st_size / (1024 ** 2)

space_saved = (
    (csv_size_mb - parquet_size_mb) / csv_size_mb
) * 100


# 7. Read CSV and measure reading time
start_time = time.perf_counter()

csv_df = pl.read_csv(CSV_PATH)

csv_read_time = time.perf_counter() - start_time


# 8. Read Parquet and measure reading time
start_time = time.perf_counter()

parquet_df = pl.read_parquet(PARQUET_PATH)

parquet_read_time = time.perf_counter() - start_time


# 9. Verify that both formats contain the same data
assert csv_df.shape == parquet_df.shape
assert csv_df.height == df.height

# 10. Display benchmark results
print("\n========== STORAGE BENCHMARK ==========")

print(f"Records: {df.height:,}")
print(f"Columns: {df.width}")

print("\nFile sizes:")
print(f"CSV:     {csv_size_mb:.2f} MB")
print(f"Parquet: {parquet_size_mb:.2f} MB")

print(f"\nStorage reduction: {space_saved:.2f}%")

print("\nWriting time:")
print(f"CSV:     {csv_write_time:.2f} seconds")
print(f"Parquet: {parquet_write_time:.2f} seconds")

print("\nReading time:")
print(f"CSV:     {csv_read_time:.2f} seconds")
print(f"Parquet: {parquet_read_time:.2f} seconds")

print("\nData verification:")
print(f"CSV shape:     {csv_df.shape}")
print(f"Parquet shape: {parquet_df.shape}")

print("\nBenchmark completed successfully!")
print(f"Output folder: {OUTPUT_DIR}")
