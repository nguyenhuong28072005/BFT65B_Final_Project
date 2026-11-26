"""
patch_notebooks_to_v3.py  -   tro cac notebook ve dataset chuan v3
va vo hieu hoa cac cell GHI DE artifact.

Script sua truc tiep cac file .ipynb that cua ban (tim cell theo noi dung, khong theo
so thu tu), SAO LUU truoc, va bao cao tung thay doi. Chay lai nhieu lan van an toan.

Cach chay (PowerShell, tai thu muc goc project, da kich hoat .venv; DONG notebook trong VS Code truoc):
    python src\\patch_notebooks_to_v3.py

Sau khi chay, cac cell bi sua duoc xoa output .
"""
import json
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

MARK = "[E3-v3]"

NEUTRAL = (
    f"# {MARK} Cell xuat file da bi VO HIEU HOA de khong ghi de artifact.\n"
    "# Dataset chuan duoc tao boi:  python src\\build_dataset.py --version v3\n"
    "# Dau ra: data\\processed\\v3\\ (panel_v3, features_v3, manifest.json)\n"
    "print('Export disabled. Canonical dataset: data/processed/v3/')\n"
)

NB03_READONLY = (
    f"# {MARK} Notebook 03 chi DOC dataset chuan v3, khong xuat file moi.\n"
    "assert feature_path.name == 'bank_quarter_features_v3.parquet', feature_path\n"
    "assert feature_path.exists(), feature_path\n"
    "print('Dataset (read-only):', feature_path)\n"
    "print('Shape in memory:', features.shape)\n"
    "print('Columns added in this notebook only (not saved):',\n"
    "      [c for c in features.columns if c == 'QUARTERLY_NETINC_TO_ASSETS'])\n"
)

AUDIT_ONLY = (
    "# [E1-v3] CHI DE AUDIT. Cell nay tinh ty so theo PHAN SO (khong nhan 100) va co the ghi de\n"
    "# bien `temporal_features`. Feature CHUAN (don vi %) duoc tao boi src/build_dataset.py.\n"
)

V3_FEATURES_REL = '/ "v3"\n    / "bank_quarter_features_v3.parquet"'

PATCHES = [
    # ---------------- Notebook 01 ----------------
    dict(nb="01_data_exploration.ipynb", name="NB01: xuat panel (ghi de panel cu)",
         find=["bank_quarter_panel.parquet", "write_parquet"], mode="replace_cell", text=NEUTRAL),
    dict(nb="01_data_exploration.ipynb", name="NB01: xuat features (cell bi hong cu phap)",
         find=["bank_quarter_features.parquet", "temporal_features.write_parquet"],
         mode="replace_cell", text=NEUTRAL),
    dict(nb="01_data_exploration.ipynb", name="NB01: xuat Parquet phan vung (se lam lai o Goi 4)",
         find=["optimized_df.write_parquet"], mode="replace_cell", text=NEUTRAL),
    dict(nb="01_data_exploration.ipynb", name="NB01: cell REBUILD (ty so phan so)",
         find=["REBUILD FINANCIAL AND TEMPORAL FEATURES FROM netinc_features"],
         mode="prepend", text=AUDIT_ONLY),
    dict(nb="01_data_exploration.ipynb", name="NB01: cell audit change features",
         find=["Audit missing values in quarter-to-quarter features"],
         mode="prepend", text=AUDIT_ONLY),
    # ---------------- Notebook 02 ----------------
    dict(nb="02_data_preprocessing.ipynb", name="NB02: doc features v3",
         find=['feature_path = PROCESSED_DIR / "bank_quarter_features.parquet"'], mode="replace",
         pairs=[('PROCESSED_DIR / "bank_quarter_features.parquet"',
                 'PROCESSED_DIR / "v3" / "bank_quarter_features_v3.parquet"'),
                ('PROCESSED_DIR.glob("*.parquet")', '(PROCESSED_DIR / "v3").glob("*.parquet")')]),
    dict(nb="02_data_preprocessing.ipynb", name="NB02: doc panel v3",
         find=['panel_path = PROCESSED_DIR / "bank_quarter_panel.parquet"'], mode="replace",
         pairs=[('PROCESSED_DIR / "bank_quarter_panel.parquet"',
                 'PROCESSED_DIR / "v3" / "bank_quarter_panel_v3.parquet"')]),
    # ---------------- Notebook 03 ----------------
    dict(nb="03_feature_engineering.ipynb", name="NB03: markdown ghi input v3",
         find=["data/processed/bank_quarter_features.parquet"], ctype="markdown", mode="replace",
         pairs=[("data/processed/bank_quarter_features.parquet",
                 "data/processed/v3/bank_quarter_features_v3.parquet")]),
    dict(nb="03_feature_engineering.ipynb", name="NB03: doc features v3",
         find=['feature_path = PROCESSED_DIR / "bank_quarter_features.parquet"',
               "pl.read_parquet(feature_path)"], mode="replace",
         pairs=[('PROCESSED_DIR / "bank_quarter_features.parquet"',
                 'PROCESSED_DIR / "v3" / "bank_quarter_features_v3.parquet"')]),
    dict(nb="03_feature_engineering.ipynb", name="NB03: xuat features_v2 (tao artifact thu 3)",
         find=["features.write_parquet(feature_v2_path"], mode="replace_cell", text=NB03_READONLY),
    # ---------------- Notebook 04 ----------------
    dict(nb="04_modeling.ipynb", name="NB04: doc features v3",
         find=['/ "bank_quarter_features_v2.parquet"', "figure_dir = project_root"], mode="replace",
         pairs=[('/ "bank_quarter_features_v2.parquet"', V3_FEATURES_REL)]),
    dict(nb="04_modeling.ipynb", name="NB04: GHI DE bank_quarter_features.parquet (E3)",
         find=["mature_data.write_parquet("], mode="replace_cell", text=NEUTRAL),
    # ---------------- Notebook 05 ----------------
    dict(nb="05_scalability_comparison.ipynb", name="NB05: doc features v3 (2 cell)",
         find=['/ "bank_quarter_features_v2.parquet"'], mode="replace", expect=2,
         pairs=[('/ "bank_quarter_features_v2.parquet"', V3_FEATURES_REL)]),
]

LEFTOVER_PATTERNS = [
    r"bank_quarter_features\.parquet", r"bank_quarter_features_v2", r"bank_quarter_panel\.parquet",
    r"\.write_parquet", r"C:\\Users",
]


def find_root() -> Path:
    for c in [Path.cwd(), Path.cwd().parent, Path(__file__).resolve().parent.parent]:
        if (c / "notebooks").is_dir() and (c / "src").is_dir():
            return c
    raise FileNotFoundError("Chay lenh tai thu muc goc project (co notebooks\\ va src\\).")


def get_src(cell) -> str:
    s = cell.get("source", "")
    return "".join(s) if isinstance(s, list) else s


def set_src(cell, text: str, clear_outputs: bool):
    cell["source"] = text.splitlines(keepends=True)
    if cell["cell_type"] == "code" and clear_outputs:
        cell["outputs"] = []
        cell["execution_count"] = None


def main():
    root = find_root()
    nb_dir = root / "notebooks"
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = nb_dir / f"_backup_{stamp}"
    notebooks = {}
    for p in sorted(nb_dir.glob("0*.ipynb")):
        notebooks[p.name] = json.loads(p.read_text(encoding="utf-8"))
    print("Notebooks tim thay:", list(notebooks))

    results = []
    changed = set()
    for patch in PATCHES:
        nb = notebooks.get(patch["nb"])
        if nb is None:
            results.append((patch["name"], "KHONG CO FILE NOTEBOOK"))
            continue
        ctype = patch.get("ctype", "code")
        expect = patch.get("expect", 1)
        hits = [i for i, c in enumerate(nb["cells"])
                if c["cell_type"] == ctype and all(f in get_src(c) for f in patch["find"])]
        if len(hits) != expect:
            results.append((patch["name"],
                            f"BO QUA: tim thay {len(hits)} cell, can {expect} "
                            f"(co the da va truoc do hoac notebook da thay doi)"))
            continue
        if patch["mode"] == "prepend" and all("[E1-v3]" in get_src(nb["cells"][i]) for i in hits):
            results.append((patch["name"], "DA VA TRUOC DO (bo qua)"))
            continue
        for i in hits:
            cell = nb["cells"][i]
            src = get_src(cell)
            if patch["mode"] == "replace_cell":
                set_src(cell, patch["text"], clear_outputs=True)
            elif patch["mode"] == "prepend":
                set_src(cell, patch["text"] + src, clear_outputs=False)
            elif patch["mode"] == "replace":
                for old, new in patch["pairs"]:
                    if old not in src:
                        raise RuntimeError(f"{patch['name']}: khong thay '{old}' trong cell {i}")
                    src = src.replace(old, new)
                set_src(cell, src, clear_outputs=True)
        changed.add(patch["nb"])
        results.append((patch["name"], f"DA SUA cell so {hits}"))

    if changed:
        backup.mkdir(exist_ok=True)
        for name in sorted(changed):
            shutil.copy2(nb_dir / name, backup / name)
        for name in sorted(changed):
            (nb_dir / name).write_text(
                json.dumps(notebooks[name], ensure_ascii=False, indent=1) + "\n", encoding="utf-8")
        print(f"\nDa sao luu file goc vao: {backup}")

    print("\n=== KET QUA TUNG THAY DOI ===")
    for name, status in results:
        print(f"- {name}: {status}")

    print("\n=== CON SOT LAI (chua xu ly o goi nay, chi de ban biet) ===")
    pat = re.compile("|".join(LEFTOVER_PATTERNS))
    n_left = 0
    for name, nb in notebooks.items():
        for i, c in enumerate(nb["cells"]):
            if c["cell_type"] != "code":
                continue
            for line in get_src(c).split("\n"):
                if pat.search(line) and MARK not in line:
                    n_left += 1
                    print(f"{name} cell {i}: {line.strip()[:110]}")
    print(f"Tong so dong con sot: {n_left}")
    print("\nHOAN TAT.")


if __name__ == "__main__":
    main()
