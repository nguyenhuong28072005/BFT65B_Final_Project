# Big-Data Pipeline for Bank Failure Early Warning (BFT65B)

## 1. Student & Course Information
- **Course:** Big Data Analytics in Finance (BFT65B) – National Economics University (NEU)
- **Author:** Nguyen Thi Huong (Student ID: 11230971)
- **Topic:** A Big-Data Pipeline for Bank Failure Early Warning
- **Repository URL:** https://github.com/nguyenhuong28072005/BFT65B_Final_Project

## 2. Pipeline Architecture Overview
This repository implements an end-to-end data-engineering and machine-learning pipeline built on over four decades of U.S. FDIC quarterly Call Reports (1984Q1–2026Q2), bank failure records, and institution history:
1. **Acquisition & Storage:** Reproducible retrieval via the public FDIC BankFind API, stored in typed, compressed Parquet format.
2. **Data Quality & Point-in-Time Panel (Polars):** Automated quality audits, resolution of cumulative year-to-date net income, non-failure merger censoring (change codes `213` and `223`), and four-quarter-ahead failure labeling `(q, q+4]`.
3. **Scalable Processing & Modeling:** Engine benchmarking (Polars vs. Pandas vs. PySpark) and early-warning modeling (Scikit-learn & PySpark MLlib) evaluated under a chronological split with 4-quarter purge gaps to prevent look-ahead bias.

## 3. Repository Structure
```text
├── data/
│   ├── raw/                  # Raw FDIC data (excluded via .gitignore)
│   └── processed/v3/         # Cleaned panel, features, and manifest.json (excluded)
├── notebooks/
│   ├── 01_data_exploration.ipynb        # Data quality audit & exploratory analysis
│   ├── 02_data_preprocessing.ipynb      # Panel construction & merger censoring checks
│   ├── 03_feature_engineering.ipynb     # Backward-looking CAMELS feature extraction
│   ├── 04_modeling.ipynb                # Model training, validation & test evaluation
│   └── 05_scalability_comparison.ipynb  # Benchmark: Polars vs. Pandas vs. PySpark
├── src/
│   ├── build_dataset.py      # Core end-to-end dataset & feature pipeline (--version v3)
│   ├── audit_artifacts.py    # Automated schema, key, checksum, and label verification
│   ├── benchmark_storage.py  # Storage layout & compression benchmark (CSV vs. Parquet)
│   └── check_*.py            # Auxiliary audit scripts for failures, mergers, and gaps
├── reports/                  # Generated summary tables (CSV), audit logs, and figures
├── requirements.txt          # Pinned Python dependencies (UTF-8)
├── .gitignore                # Excludes virtual environments, cache, and raw/processed data
└── README.md                 # Reproduction instructions & disclosures
```

## 4. End-to-End Reproducibility Instructions

1. **Create a virtual environment and install pinned dependencies:**
   ```bash
   python -m venv .venv
   # Windows: .venv\Scripts\activate  |  macOS/Linux: source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Acquire raw FDIC data (not committed to Git):**
   - Retrieve the four required tables from the public FDIC BankFind API (`https://api.fdic.gov/banks`) into `data/raw/`:
     - `data/raw/financials_parts/*.parquet` (1984Q1–2026Q2, sorted by `REPDTE, CERT`)
     - `data/raw/failures/failures_enriched.parquet`
     - `data/interim/institution_history.parquet` and the Institutions table

3. **Build the point-in-time panel and feature table (`v3`):**
   ```bash
   python src/build_dataset.py --version v3
   ```
   *Output:* Generates `bank_quarter_panel_v3.parquet`, `bank_quarter_features_v3.parquet`, and `manifest.json` inside `data/processed/v3/`.

4. **Verify dataset integrity (optional audit):**
   ```bash
   python src/audit_artifacts.py
   ```
   *Output:* Writes a full SHA-256 checksum, schema, and label-consistency report to `reports/audit_artifacts_output.txt`.

5. **Run the analysis and modeling notebooks:**
   - Launch Jupyter and run notebooks `01` through `05` in sequential order using **Restart & Run All** to reproduce all tables and figures in `reports/`.

## 5. Academic Integrity & AI-Use Disclosure
- **Data & Open-Source Software:** Public data sourced from the U.S. Federal Deposit Insurance Corporation (FDIC) BankFind Suite API. Open-source libraries used include Polars, Pandas, PyArrow, Apache Spark (PySpark), Scikit-learn, NumPy, Matplotlib, and Seaborn.
- **AI-Use Disclosure:** AI assistants (Anthropic Claude and Antigravity) were used during this project to review notebook code and outputs, audit figures against saved artifacts, draft and revise sections of the report, and propose corrections to code in the accompanying notebooks. Some of these proposed corrections were applied directly to the notebooks under the author's direction. The author reviewed, ran, and takes responsibility for all code, experimental results, and final content submitted; no content from other students' work or non-attributed external sources was used.