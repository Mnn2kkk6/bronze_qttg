# QTTG BHXH — Bronze → Silver → Gold (Spark + Airflow local)

Bài thực hành xử lý 2 bảng `RAW_QTTG_BHXH` (master) và `RAW_QTTG_BHXH_DETAIL`
(detail) theo mô hình lakehouse Bronze/Silver/Gold, chạy Spark local trên
Docker Desktop (Windows), điều phối bằng Airflow.

Tài liệu nghiệp vụ gốc: `PHAN_TICH_NGHIEP_VU_QTTG_BHXH.md`
Hướng dẫn Airflow + Spark local: `HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`

## Trạng thái hiện tại

| Layer | Trạng thái |
|---|---|
| Bronze | ✅ Hoàn thành |
| Silver | ⬜ Chưa làm |
| Gold | ⬜ Chưa làm |
| Validate | ⬜ Chưa làm |
| Airflow DAG | ⬜ Chưa làm |

## Cấu trúc thư mục

```text
Datahub/
├── spark/
│   └── apps/
│       └── qttg/
│           ├── bronze_qttg.py       # Da xong
│           ├── silver_qttg.py       # Rong, chua viet
│           ├── gold_qttg.py         # Rong, chua viet
│           └── validate_qttg.py     # Rong, chua viet
├── airflow-local/
│   ├── dags/
│   │   └── dag_qttg_bronze_silver_gold.py   # Rong, chua viet
│   └── logs/
├── output/
│   ├── raw_qttg_1m/                 # 2 CSV nguon
│   │   ├── RAW_QTTG_BHXH.csv
│   │   └── RAW_QTTG_BHXH_DETAIL.csv
│   └── spark_lake/
│       ├── bronze/                  # Da co du lieu
│       ├── silver/                  # Rong
│       └── gold/                    # Rong
└── README.md
```

## Môi trường thực hành

Bronze hiện đang được chạy và kiểm chứng trực tiếp trên máy local (Windows,
`python bronze_qttg.py`), chưa qua container Spark/Airflow. Phần
Docker Compose + Airflow (theo `HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`) sẽ được
bổ sung ở bước sau.

- OS: Windows
- Python: 3.11.9
- Java: OpenJDK 17 (Microsoft build)
- PySpark: 3.5.3
- Cần `HADOOP_HOME` trỏ tới thư mục chứa `winutils.exe` + `hadoop.dll`
  (Hadoop 3.3.x) để Spark ghi được file trên Windows — nếu không có sẽ
  gặp lỗi `HADOOP_HOME and hadoop.home.dir are unset` khi ghi Parquet.

## Bronze layer

### Việc đã làm

- Đọc nguyên trạng 2 CSV với schema tường minh (khớp đúng
  `DDL_RAW_QTTG_BHXH.sql` / `DDL_RAW_QTTG_BHXH_DETAIL.sql`), không suy kiểu
  tự động, không lọc, không chọn bản ghi mới nhất.
- Thêm 3 cột metadata: `source_file`, `load_time`, `layer = "BRONZE"`.
- Ghi ra Parquet tại `output/spark_lake/bronze/raw_qttg_bhxh` và
  `output/spark_lake/bronze/raw_qttg_bhxh_detail`.
- Sau khi ghi, đọc lại từ Parquet để đếm số dòng thực tế (không đếm
  DataFrame trước khi ghi), so với số dòng input và với con số kỳ vọng.
  Nếu lệch thì job thoát với exit code khác 0.

### Lệnh chạy

```powershell
cd Datahub
python spark/apps/qttg/bronze_qttg.py `
  --input-dir output/raw_qttg_1m `
  --output-dir output/spark_lake/bronze
```

### Kết quả chạy thực tế

```text
LAYER=BRONZE STATUS=SUCCESS MASTER_ROWS=142857 DETAIL_ROWS=1000000
```

Khớp đúng số dòng input (142,857 dòng master / 1,000,000 dòng detail) và
đúng con số kỳ vọng nêu trong tài liệu nghiệp vụ.

Kiểm tra output trên đĩa:

```powershell
dir output\spark_lake\bronze\raw_qttg_bhxh
dir output\spark_lake\bronze\raw_qttg_bhxh_detail
```

Cả 2 thư mục đều có file `_SUCCESS` và nhiều file `.snappy.parquet`.

### Vấn đề đã gặp và cách xử lý

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| `PATH_NOT_FOUND` khi đọc CSV | Sai `--input-dir`, CSV nằm trong thư mục con lồng nhau | Kiểm tra lại đường dẫn thật bằng `dir`, trỏ đúng thư mục chứa 2 file CSV |
| `HADOOP_HOME and hadoop.home.dir are unset` khi ghi Parquet | Windows cần `winutils.exe`/`hadoop.dll` để Spark thao tác file, biến `HADOOP_HOME` chưa được set | Tải `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x) vào `C:\hadoop\bin`, set biến môi trường `HADOOP_HOME=C:\hadoop`, mở terminal mới để nạp lại biến |
| Biến môi trường không nhận trong terminal VS Code | VS Code chỉ đọc biến môi trường lúc khởi động, không tự refresh khi terminal system đã set biến mới | Đóng hẳn VS Code và mở lại sau khi set biến |

