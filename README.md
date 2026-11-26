# Big-Data Pipeline for Bank Failure Early Warning (BFT65B)

## 1. Thông tin sinh viên & Đề tài
- **Khóa học:** Big Data Analytics in Finance (BFT65B) - National Economics University (NEU)
- **Đề tài:** A Big-Data Pipeline for Bank Failure Early Warning
- **Mã nguồn:** [GitHub Repository URL]

## 2. Tổng quan kiến trúc Pipeline
Pipeline xử lý hơn 40 năm dữ liệu Call Reports của FDIC (1984Q1 - 2025Q2) với 3 module cốt lõi:
1. **Acquisition & Storage:** Tải tự động qua FDIC API, lưu trữ Parquet phân vùng theo năm.
2. **High-Performance Processing (Polars):** Ghép nối bảng, xử lý sáp nhập point-in-time, tạo nhãn thất bại trong cửa sổ 4 quý (q, q+4].
3. **Machine Learning & Distributed Modeling:** Huấn luyện Scikit-Learn và PySpark MLlib với cơ chế Temporal Split (Purge Gaps 1 năm) ngăn chặn triệt để Look-Ahead Bias.

## 3. Cấu trúc thư mục
```text
├── data/
│   ├── raw/                  # Chứa dữ liệu gốc tải từ FDIC API
│   └── processed/            # Chứa panel Parquet và features
├── notebooks/
│   ├── 01_data_exploration.ipynb        # Khám phá dữ liệu (EDA)
│   ├── 02_data_preprocessing.ipynb      # Tiền xử lý & Xây dựng Panel Data
│   ├── 03_feature_engineering.ipynb     # Trích xuất 14 đặc trưng CAMELS
│   ├── 04_modeling.ipynb                # Huấn luyện ML & Dynamic Threshold Tuning
│   └── 05_scalability_comparison.ipynb  # Benchmark Polars vs Pandas vs PySpark
├── src/                      # Mã nguồn Python độc lập cho pipeline tự động
├── reports/                  # Biểu đồ PNG và bảng tổng hợp CSV
├── requirements.txt          # Thư viện ghim phiên bản chính xác
└── README.md                 # Hướng dẫn chi tiết
```

## 4. Hướng dẫn chạy lại toàn bộ Pipeline (End-to-End Reproducibility)
1. **Cài đặt môi trường:**
   ```bash
   pip install -r requirements.txt
   ```
2. **Tải dữ liệu tự động:**
   ```bash
   python src/download_fdic.py
   python src/download_failures.py
   python src/download_institutions.py
   ```
3. **Thực thi Pipeline xử lý dữ liệu:**
   - Chạy tuần tự các Notebooks: `01` -> `02` -> `03` -> `04` -> `05`.
   - Hoặc chạy script Spark MLlib độc lập:
     ```bash
     python src/spark_mllib_demo.py
     ```