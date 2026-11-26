
import argparse
import hashlib
import json
import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

OBSERVATION_CUTOFF = date(2025, 6, 30)   # 4 quy cuoi truoc 2026Q2 chua du horizon
HORIZON_QUARTERS = 4
MERGER_CODES = [213, 223]
REQUIRED_RAW = ["CERT", "REPDTE", "ASSET", "DEP", "EQ", "NETINC", "ROA", "NCLNLS", "LNATRES"]
MODEL_FEATURES = [
    "ROA", "LOG_ASSET", "EQUITY_TO_ASSETS", "DEPOSITS_TO_ASSETS", "NPL_TO_ASSETS",
    "ALLOWANCE_TO_ASSETS", "ASSET_QOQ_GROWTH", "EQUITY_RATIO_CHANGE", "ROA_CHANGE",
    "NPL_RATIO_CHANGE", "ALLOWANCE_RATIO_CHANGE", "NEGATIVE_NCLNLS_FLAG",
    "NEGATIVE_LNATRES_FLAG", "EXTREME_ASSET_GROWTH_FLAG",
]


def find_root() -> Path:
    for c in [Path.cwd(), Path.cwd().parent, Path(__file__).resolve().parent.parent]:
        if (c / "data" / "raw" / "financials_parts").exists():
            return c
    raise FileNotFoundError(
        "Khong tim thay data/raw/financials_parts. Chay lenh tai thu muc goc project."
    )


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def qidx(date_col: str) -> pl.Expr:
    """Chi so quy = nam*4 + (thang-1)//3  (cung cong thuc voi NB01 cell 49)."""
    return pl.col(date_col).dt.year() * 4 + ((pl.col(date_col).dt.month() - 1) // 3)


# --------------------------------------------------------------------------
# 1. Load raw
# --------------------------------------------------------------------------
def load_financials(root: Path) -> pl.DataFrame:
    files = sorted((root / "data" / "raw" / "financials_parts").glob("*.parquet"))
    if not files:
        raise FileNotFoundError("Khong co file parquet trong data/raw/financials_parts")
    fin = pl.concat([pl.read_parquet(f) for f in files], how="vertical_relaxed")
    missing = [c for c in REQUIRED_RAW if c not in fin.columns]
    assert not missing, f"Thieu cot raw: {missing}"
    fin = fin.with_columns(
        pl.col("CERT").cast(pl.Int64),
        pl.col("REPDTE").cast(pl.Utf8).str.strptime(pl.Date, format="%Y%m%d", strict=True)
        .alias("REPORT_DATE"),
    ).with_columns(qidx("REPORT_DATE").alias("REPORT_Q_INDEX"))
    dup = fin.group_by(["CERT", "REPORT_DATE"]).len().filter(pl.col("len") > 1).height
    assert dup == 0, f"Co {dup} khoa (CERT, REPORT_DATE) trung lap trong raw Financials"
    print(f"[1] Financials: {fin.height:,} rows, {fin.width} cols, "
          f"{fin['CERT'].n_unique():,} banks, {len(files)} files")
    return fin


def load_first_failures(root: Path) -> pl.DataFrame:
    path = root / "data" / "raw" / "failures" / "failures_enriched.parquet"
    fail = pl.read_parquet(path)
    events = (
        fail.filter(pl.col("RESTYPE") == "FAILURE")
        .with_columns(pl.col("FAILDATE").str.to_date("%m/%d/%Y", strict=False)
                      .alias("FAILURE_DATE"))
        .filter(pl.col("CERT").is_not_null())
        .with_columns(pl.col("CERT").cast(pl.Int64))
    )
    assert events["FAILURE_DATE"].null_count() == 0, "Co FAILDATE khong parse duoc"
    first = (events.group_by("CERT").agg(pl.col("FAILURE_DATE").min().alias("FIRST_FAILURE_DATE"))
             .with_columns(qidx("FIRST_FAILURE_DATE").alias("FAILURE_Q_INDEX"))
             .sort("CERT"))
    print(f"[2] Failure events (RESTYPE=FAILURE, co CERT): {events.height:,}; "
          f"banks: {first.height:,}")
    return first


def load_merger_events(root: Path, first_failure: pl.DataFrame) -> pl.DataFrame:
    path = root / "data" / "interim" / "institution_history.parquet"
    h = pl.read_parquet(path).with_columns(
        pl.col("EFFDATE").str.to_datetime(strict=False).dt.date().alias("EFFECTIVE_DATE"),
        pl.col("CERT").cast(pl.Int64, strict=False),
    )
    m = (
        h.filter(pl.col("CHANGECODE").is_in(MERGER_CODES) & pl.col("CERT").is_not_null())
        .join(first_failure.select("CERT", "FIRST_FAILURE_DATE"), on="CERT", how="left")
        # Chi giu merger KHONG phai failure: khong co failure, hoac merger xay ra truoc failure
        .filter(pl.col("FIRST_FAILURE_DATE").is_null()
                | (pl.col("EFFECTIVE_DATE") < pl.col("FIRST_FAILURE_DATE")))
        .with_columns(qidx("EFFECTIVE_DATE").alias("MERGER_Q_INDEX"))
        .select("CERT", "EFFECTIVE_DATE", "MERGER_Q_INDEX")
    )
    print(f"[3] Merger events du dieu kien censoring: {m.height:,}; banks: {m['CERT'].n_unique():,}")
    return m


# --------------------------------------------------------------------------
# 2. Panel + target + censoring
# --------------------------------------------------------------------------
def build_panel(fin: pl.DataFrame, first_failure: pl.DataFrame, mergers: pl.DataFrame) -> pl.DataFrame:
    panel = (
        fin.join(first_failure, on="CERT", how="left")
        .filter(pl.col("REPORT_DATE") <= OBSERVATION_CUTOFF)
        .filter(pl.col("FIRST_FAILURE_DATE").is_null()
                | (pl.col("REPORT_DATE") < pl.col("FIRST_FAILURE_DATE")))
        .with_columns(
            (pl.col("FAILURE_Q_INDEX").is_not_null()
             & (pl.col("FAILURE_Q_INDEX") > pl.col("REPORT_Q_INDEX"))
             & (pl.col("FAILURE_Q_INDEX") <= pl.col("REPORT_Q_INDEX") + HORIZON_QUARTERS))
            .cast(pl.Int8).alias("FAILURE_LABEL")
        )
    )
    censored_keys = (
        panel.filter(pl.col("FAILURE_LABEL") == 0)
        .join(mergers, on="CERT", how="inner")
        .filter((pl.col("MERGER_Q_INDEX") > pl.col("REPORT_Q_INDEX"))
                & (pl.col("MERGER_Q_INDEX") <= pl.col("REPORT_Q_INDEX") + HORIZON_QUARTERS))
        .select("CERT", "REPORT_DATE").unique()
        .with_columns(pl.lit(True).alias("IS_MERGER_CENSORED"))
    )
    panel = (
        panel.join(censored_keys, on=["CERT", "REPORT_DATE"], how="left")
        .with_columns(pl.col("IS_MERGER_CENSORED").fill_null(False))
        .with_columns(
            pl.when(pl.col("IS_MERGER_CENSORED")).then(pl.lit(None, dtype=pl.Int8))
            .otherwise(pl.col("FAILURE_LABEL")).alias("FINAL_LABEL")
        )
        .sort(["CERT", "REPORT_DATE"])
    )
    n0 = panel.filter(pl.col("FINAL_LABEL") == 0).height
    n1 = panel.filter(pl.col("FINAL_LABEL") == 1).height
    nn = panel.filter(pl.col("FINAL_LABEL").is_null()).height
    print(f"[4] Panel: {panel.height:,} rows | label0={n0:,} label1={n1:,} censored(null)={nn:,}")
    assert n0 + n1 + nn == panel.height
    assert nn == int(panel["IS_MERGER_CENSORED"].sum()), "Null label khac so dong censored"
    assert panel["REPORT_DATE"].max() <= OBSERVATION_CUTOFF
    leak = panel.filter(pl.col("FIRST_FAILURE_DATE").is_not_null()
                        & (pl.col("REPORT_DATE") >= pl.col("FIRST_FAILURE_DATE"))).height
    assert leak == 0, "Con quan sat tai/sau ngay failure"
    return panel


# --------------------------------------------------------------------------
# 3. Features (chi dung thong tin qua khu: shift(1) theo CERT, da sort theo thoi gian)
# --------------------------------------------------------------------------
def build_features(panel: pl.DataFrame) -> pl.DataFrame:
    f = (
        panel.sort(["CERT", "REPORT_DATE"])
        .with_columns(
            pl.col("REPORT_DATE").dt.year().alias("YEAR"),
            pl.col("REPORT_DATE").dt.quarter().alias("QUARTER"),
        )
        .with_columns(
            pl.col("NETINC").shift(1).over(["CERT", "YEAR"]).alias("PREV_NETINC_YTD"),
            pl.col("QUARTER").shift(1).over(["CERT", "YEAR"]).alias("PREV_QUARTER"),
        )
        .with_columns(
            # NETINC trong FDIC la luy ke tu dau nam (YTD) -> doi sang thu nhap rieng tung quy
            pl.when(pl.col("QUARTER") == 1).then(pl.col("NETINC"))
            .when((pl.col("QUARTER") > 1) & (pl.col("PREV_QUARTER") == pl.col("QUARTER") - 1))
            .then(pl.col("NETINC") - pl.col("PREV_NETINC_YTD"))
            .otherwise(None).alias("QUARTERLY_NETINC")
        )
        .with_columns(
            # [GUARD] chi tinh khi ASSET > 0
            pl.when(pl.col("ASSET") > 0).then(pl.col("ASSET").log()).otherwise(None).alias("LOG_ASSET"),
            pl.when(pl.col("ASSET") > 0).then(pl.col("EQ") / pl.col("ASSET") * 100)
            .otherwise(None).alias("EQUITY_TO_ASSETS"),
            pl.when(pl.col("ASSET") > 0).then(pl.col("DEP") / pl.col("ASSET") * 100)
            .otherwise(None).alias("DEPOSITS_TO_ASSETS"),
            (pl.col("NCLNLS") < 0).fill_null(False).alias("NEGATIVE_NCLNLS_FLAG"),
            (pl.col("LNATRES") < 0).fill_null(False).alias("NEGATIVE_LNATRES_FLAG"),
        )
        .with_columns(
            pl.when((pl.col("NCLNLS") >= 0) & (pl.col("ASSET") > 0))
            .then(pl.col("NCLNLS") / pl.col("ASSET") * 100).otherwise(None).alias("NPL_TO_ASSETS"),
            pl.when((pl.col("LNATRES") >= 0) & (pl.col("ASSET") > 0))
            .then(pl.col("LNATRES") / pl.col("ASSET") * 100).otherwise(None)
            .alias("ALLOWANCE_TO_ASSETS"),
        )
        .with_columns(
            pl.col("REPORT_Q_INDEX").shift(1).over("CERT").alias("_PREV_Q"),
            pl.col("ASSET").shift(1).over("CERT").alias("_PREV_ASSET"),
            pl.col("EQUITY_TO_ASSETS").shift(1).over("CERT").alias("_PREV_EQ"),
            pl.col("ROA").shift(1).over("CERT").alias("_PREV_ROA"),
            pl.col("NPL_TO_ASSETS").shift(1).over("CERT").alias("_PREV_NPL"),
            pl.col("ALLOWANCE_TO_ASSETS").shift(1).over("CERT").alias("_PREV_ALW"),
        )
        .with_columns((pl.col("REPORT_Q_INDEX") == pl.col("_PREV_Q") + 1).alias("_CONSEC"))
        .with_columns(
            # [GUARD] PREV_ASSET > 0
            pl.when(pl.col("_CONSEC") & (pl.col("_PREV_ASSET") > 0))
            .then((pl.col("ASSET") / pl.col("_PREV_ASSET") - 1) * 100)
            .otherwise(None).alias("ASSET_QOQ_GROWTH"),
            pl.when(pl.col("_CONSEC")).then(pl.col("EQUITY_TO_ASSETS") - pl.col("_PREV_EQ"))
            .otherwise(None).alias("EQUITY_RATIO_CHANGE"),
            pl.when(pl.col("_CONSEC")).then(pl.col("ROA") - pl.col("_PREV_ROA"))
            .otherwise(None).alias("ROA_CHANGE"),
            pl.when(pl.col("_CONSEC")).then(pl.col("NPL_TO_ASSETS") - pl.col("_PREV_NPL"))
            .otherwise(None).alias("NPL_RATIO_CHANGE"),
            pl.when(pl.col("_CONSEC")).then(pl.col("ALLOWANCE_TO_ASSETS") - pl.col("_PREV_ALW"))
            .otherwise(None).alias("ALLOWANCE_RATIO_CHANGE"),
        )
        .with_columns(
            ((pl.col("ASSET_QOQ_GROWTH") > 100) | (pl.col("ASSET_QOQ_GROWTH") < -50))
            .fill_null(False).alias("EXTREME_ASSET_GROWTH_FLAG")
        )
        .drop([c for c in ["_PREV_Q", "_PREV_ASSET", "_PREV_EQ", "_PREV_ROA", "_PREV_NPL",
                           "_PREV_ALW", "_CONSEC"]])
    )
    missing = [c for c in MODEL_FEATURES if c not in f.columns]
    assert not missing, f"Thieu feature: {missing}"
    med = f["EQUITY_TO_ASSETS"].median()
    assert 1 < med < 30, f"EQUITY_TO_ASSETS median={med}: don vi khong phai phan tram"
    print(f"[5] Features: {f.height:,} rows, {f.width} cols | median EQUITY_TO_ASSETS = {med:.4f} (%)")
    return f


# --------------------------------------------------------------------------
# 4. Export co phien ban, khong ghi de
# --------------------------------------------------------------------------
def export(df: pl.DataFrame, path: Path) -> dict:
    if path.exists():
        raise FileExistsError(f"{path} da ton tai. Doi --version hoac xoa thu cong neu chac chan.")
    df.write_parquet(path, compression="zstd")
    back = pl.read_parquet(path)
    assert back.shape == df.shape, "Shape sau khi doc lai khac shape luc ghi"
    info = {
        "file": path.name, "rows": back.height, "cols": back.width,
        "size_mib": round(path.stat().st_size / (1024 ** 2), 2),
        "sha256": sha256_of(path), "columns": back.columns,
    }
    print(f"    ghi {path.name}: {info['rows']:,} x {info['cols']} | {info['size_mib']} MiB")
    return info


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default="v3", help="ten thu muc phien ban, vd v3")
    args = ap.parse_args()

    root = find_root()
    out_dir = root / "data" / "processed" / args.version
    out_dir.mkdir(parents=True, exist_ok=True)

    fin = load_financials(root)
    first_failure = load_first_failures(root)
    mergers = load_merger_events(root, first_failure)
    panel = build_panel(fin, first_failure, mergers)
    feats = build_features(panel)

    print("[6] Export")
    manifest = {
        "created": datetime.now().isoformat(timespec="seconds"),
        "version": args.version,
        "observation_cutoff": "2025-06-30",
        "horizon_quarters": HORIZON_QUARTERS,
        "merger_codes_censored": MERGER_CODES,
        "model_features": MODEL_FEATURES,
        "label_counts": {
            "label0": int(feats.filter(pl.col("FINAL_LABEL") == 0).height),
            "label1": int(feats.filter(pl.col("FINAL_LABEL") == 1).height),
            "censored_null": int(feats.filter(pl.col("FINAL_LABEL").is_null()).height),
        },
        "n_banks": int(feats["CERT"].n_unique()),
        "date_min": str(feats["REPORT_DATE"].min()),
        "date_max": str(feats["REPORT_DATE"].max()),
        "files": [
            export(panel, out_dir / f"bank_quarter_panel_{args.version}.parquet"),
            export(feats, out_dir / f"bank_quarter_features_{args.version}.parquet"),
        ],
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False),
                                           encoding="utf-8")
    print("\nHOAN TAT. Manifest:", out_dir / "manifest.json")
    print(json.dumps({k: manifest[k] for k in ["label_counts", "n_banks", "date_min", "date_max"]},
                     indent=2))


if __name__ == "__main__":
    main()
