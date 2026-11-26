from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import polars as pl

KEYS = ["CERT", "REPORT_DATE"]
MODEL_FEATURES = [
    "ROA", "LOG_ASSET", "EQUITY_TO_ASSETS", "DEPOSITS_TO_ASSETS",
    "NPL_TO_ASSETS", "ALLOWANCE_TO_ASSETS", "ASSET_QOQ_GROWTH",
    "EQUITY_RATIO_CHANGE", "ROA_CHANGE", "NPL_RATIO_CHANGE",
    "ALLOWANCE_RATIO_CHANGE", "NEGATIVE_NCLNLS_FLAG",
    "NEGATIVE_LNATRES_FLAG", "EXTREME_ASSET_GROWTH_FLAG",
]
LINES: list[str] = []


def log(*parts):
    message = " ".join(str(part) for part in parts)
    print(message, flush=True)
    LINES.append(message)


def find_root() -> Path:
    script_dir = Path(__file__).resolve().parent
    for candidate in [Path.cwd(), script_dir, script_dir.parent, script_dir.parent.parent]:
        if (candidate / "data" / "processed").is_dir():
            return candidate
    raise FileNotFoundError(
        "Khong tim thay data/processed. Hay dat script trong thu muc goc BFT65B_Final_Project."
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def count_duplicates(df: pl.DataFrame):
    if not all(col in df.columns for col in KEYS):
        return "N/A"
    return df.height - df.select(KEYS).unique().height


def get_label_counts(df: pl.DataFrame) -> dict:
    if "FINAL_LABEL" not in df.columns:
        return {}
    result = {}
    for label, count in df.group_by("FINAL_LABEL").len().iter_rows():
        result["NULL" if label is None else str(label)] = int(count)
    return result


def inspect_file(path: Path) -> pl.DataFrame:
    log("")
    log("-" * 72)
    log("FILE:", path)
    log("SIZE_MB:", round(path.stat().st_size / (1024 * 1024), 2))
    log("SHA256:", sha256(path))
    df = pl.read_parquet(path)
    log("SHAPE:", f"{df.height:,} rows x {df.width} columns")
    log("COLUMNS:", ", ".join(df.columns))
    if "REPORT_DATE" in df.columns:
        log("REPORT_DATE_MIN:", df["REPORT_DATE"].min())
        log("REPORT_DATE_MAX:", df["REPORT_DATE"].max())
    if "CERT" in df.columns:
        log("UNIQUE_BANKS:", df["CERT"].n_unique())
    log("DUPLICATE_CERT_REPORT_DATE:", count_duplicates(df))
    counts = get_label_counts(df)
    if counts:
        log("FINAL_LABEL_COUNTS:", json.dumps(counts, ensure_ascii=False))
    if "IS_MERGER_CENSORED" in df.columns:
        n_censored = int(df["IS_MERGER_CENSORED"].fill_null(False).sum())
        log("IS_MERGER_CENSORED_TRUE:", n_censored)
    for col in ["EQUITY_TO_ASSETS", "ROA", "LOG_ASSET", "NPL_TO_ASSETS"]:
        if col in df.columns:
            log(f"{col}_MEDIAN:", df[col].median())
    return df


def compare_panel_features(panel: pl.DataFrame, features: pl.DataFrame):
    log("")
    log("=" * 72)
    log("PANEL VS FEATURES")
    required = KEYS + ["FINAL_LABEL"]
    for name, df in [("PANEL", panel), ("FEATURES", features)]:
        missing = [col for col in required if col not in df.columns]
        log(name, "ROWS:", df.height, "| DUPLICATE_KEYS:", count_duplicates(df),
            "| MISSING_REQUIRED_COLUMNS:", missing)

  if panel.height != features.height:
        log("WARNING: ROW_COUNTS_DIFFER:", panel.height, features.height)
    else:
        log("ROW_COUNTS_MATCH: YES")

    if any(col not in panel.columns for col in required) or any(
        col not in features.columns for col in required
    ):
        log("KEY_LABEL_COMPARISON: SKIPPED (missing required columns)")
        return

    left = panel.select(required).rename({"FINAL_LABEL": "LABEL_PANEL"}).with_columns(
        pl.lit(True).alias("IN_PANEL")
    )
    right = features.select(required).rename({"FINAL_LABEL": "LABEL_FEATURES"}).with_columns(
        pl.lit(True).alias("IN_FEATURES")
    )
    joined = left.join(right, on=KEYS, how="full", coalesce=True)
    only_panel = joined.filter(pl.col("IN_PANEL").is_not_null() & pl.col("IN_FEATURES").is_null()).height
    only_features = joined.filter(pl.col("IN_PANEL").is_null() & pl.col("IN_FEATURES").is_not_null()).height
    both = joined.filter(pl.col("IN_PANEL").is_not_null() & pl.col("IN_FEATURES").is_not_null())
    mismatch = both.filter(
        (pl.col("LABEL_PANEL").is_null() != pl.col("LABEL_FEATURES").is_null())
        | (
            pl.col("LABEL_PANEL").is_not_null()
            & pl.col("LABEL_FEATURES").is_not_null()
            & (pl.col("LABEL_PANEL") != pl.col("LABEL_FEATURES"))
        )
    ).height
    log("KEYS_ONLY_PANEL:", only_panel)
    log("KEYS_ONLY_FEATURES:", only_features)
    log("KEYS_IN_BOTH:", both.height)
    log("LABEL_MISMATCH:", mismatch)
    log("KEYS_AND_LABELS_MATCH:", "YES" if only_panel == only_features == mismatch == 0 else "NO")


def inspect_features(df: pl.DataFrame):
    log("")
    log("=" * 72)
    log("MODEL FEATURE CHECK")
    missing = [col for col in MODEL_FEATURES if col not in df.columns]
    log("EXPECTED_FEATURE_COUNT:", len(MODEL_FEATURES))
    log("MISSING_EXPECTED_FEATURES:", missing)
    for col in MODEL_FEATURES:
        if col not in df.columns:
            continue
        series = df[col]
        nan_count = 0
        if series.dtype in (pl.Float32, pl.Float64):
            nan_count = int(series.is_nan().sum())
        log(
            f"{col}: dtype={series.dtype}; nulls={series.null_count()}; "
            f"NaN={nan_count}; median={series.median()}"
        )
    if "EQUITY_TO_ASSETS" in df.columns:
        median = df["EQUITY_TO_ASSETS"].median()
        log("EQUITY_TO_ASSETS_MEDIAN_REFERENCE_1_TO_30:",
            "PASS" if median is not None and 1 <= median <= 30 else f"CHECK ({median})")


def inspect_labels(df: pl.DataFrame):
    log("")
    log("=" * 72)
    log("LABEL AND CENSORING CHECK")
    if "FINAL_LABEL" not in df.columns:
        log("FINAL_LABEL: MISSING")
        return
    counts = get_label_counts(df)
    log("LABEL_COUNTS:", json.dumps(counts, ensure_ascii=False))
    log("LABEL_COUNT_SUM:", sum(counts.values()), "| ROWS:", df.height)
    null_labels = df["FINAL_LABEL"].null_count()
    log("FINAL_LABEL_NULL:", null_labels)
    if "IS_MERGER_CENSORED" in df.columns:
        censored = int(df["IS_MERGER_CENSORED"].fill_null(False).sum())
        log("MERGER_CENSORED_TRUE:", censored)
        log("NULL_LABELS_EQUAL_CENSORED:", "YES" if null_labels == censored else "NO - REVIEW")


def main() -> int:
    root = find_root()
    version_dir = root / "data" / "processed" / "v3"
    panel_path = version_dir / "bank_quarter_panel_v3.parquet"
    features_path = version_dir / "bank_quarter_features_v3.parquet"

    log("AUDIT SCRIPT STARTED")
    log("PROJECT_ROOT:", root)
    log("VERSION_DIR:", version_dir)
    if not panel_path.is_file():
        raise FileNotFoundError(f"Khong tim thay: {panel_path}")
    if not features_path.is_file():
        raise FileNotFoundError(f"Khong tim thay: {features_path}")

    panel = inspect_file(panel_path)
    features = inspect_file(features_path)
    compare_panel_features(panel, features)
    inspect_features(features)
    inspect_labels(panel)

    report_dir = root / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / "audit_artifacts_output.txt"
    log("")
    log("AUDIT COMPLETED")
    log("REPORT_PATH:", report_path)
    report_path.write_text("\n".join(LINES) + "\n", encoding="utf-8")
    print("\nDa luu bao cao audit:", report_path, flush=True)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print("AUDIT ERROR:", repr(error), file=sys.stderr, flush=True)
        raise
