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
| Silver | ✅ Hoàn thành |
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
│           ├── silver_qttg.py       # Da xong
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
│       │   ├── raw_qttg_bhxh
│       │   └── raw_qttg_bhxh_detail
│       ├── silver/                  # Da co du lieu
│       │   ├── qttg_bhxh
│       │   ├── qttg_bhxh_detail
│       │   └── qttg_bhxh_detail_rejects   # Chi xuat hien neu co dong loi
│       └── gold/                    # Rong
└── README.md
```

## Môi trường thực hành

Bronze và Silver hiện đang được chạy và kiểm chứng trực tiếp trên máy local
(Windows, `python <script>.py`), chưa qua container Spark/Airflow. Phần
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
- Sau khi ghi, đọc lại từ Parquet để đếm số dòng thực tế, so với số dòng
  input và với con số kỳ vọng. Nếu lệch thì job thoát với exit code khác 0.

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

## Silver layer

### Việc đã làm

- **Master**: chọn bản ghi mới nhất của mỗi người bằng
  `ROW_NUMBER() OVER (PARTITION BY SO_SO_BHXH ORDER BY CREATED_AT DESC, ID DESC)`,
  giữ `RN = 1`. Thêm cột `IS_DELETED` (mặc định `0`) và `DELETED_AT` (mặc
  định `NULL`) để dự phòng cho soft-delete khi chạy incremental thật sau
  này (batch hiện tại là rebuild toàn bộ bằng overwrite).
- **Detail**: chỉ giữ các dòng có `MASTER_ID` thuộc đúng các master ID mới
  nhất vừa chọn ở trên (không lấy toàn bộ detail Bronze), đồng thời kiểm
  tra chéo `NLD_ID` giữa detail và master phải khớp nhau.
- **Clean & validate detail**:
  - `trim()` chuẩn hóa `TU_THANG`, `DEN_THANG`.
  - Validate format `YYYYMM` bằng regex `^[0-9]{4}(0[1-9]|1[0-2])$`
    (đúng 6 số, tháng 01–12) và kiểm tra `TU_THANG <= DEN_THANG`.
  - Cast tường minh `MUC_LUONG` về kiểu số (`DoubleType`).
  - Dòng không hợp lệ **không bị xóa âm thầm** — được tách riêng ra
    `qttg_bhxh_detail_rejects` kèm cột `REJECT_REASON`.
- **Kiểm tra sau ghi** (job fail nếu sai):
  - Không có `SO_SO_BHXH` nào bị trùng > 1 dòng ở Silver.
  - Không có detail mồ côi (join `left_anti` giữa detail Silver và master
    Silver theo `MASTER_ID`, phải ra 0 dòng).

### Lệnh chạy

```powershell
cd Datahub
python spark/apps/qttg/silver_qttg.py `
  --bronze-dir output/spark_lake/bronze `
  --output-dir output/spark_lake/silver
```

### Kết quả chạy thực tế

```text
LAYER=SILVER STATUS=SUCCESS MASTER_ROWS=21838 DETAIL_ROWS=169658 DETAIL_REJECTED=0
```

- 142,857 dòng master Bronze thực chất chỉ là **21,838 người phân biệt**
  (mỗi người trung bình có ~6.5 phiên bản `CREATED_AT` khác nhau) — Silver
  gom đúng còn 1 dòng/người.
- Detail giảm từ 1,000,000 xuống còn **169,658 dòng** vì chỉ giữ giai đoạn
  thuộc đúng phiên bản master mới nhất của từng người.
- 0 dòng detail bị loại do sai format tháng.

### Vấn đề đã gặp và cách xử lý

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| `PATH_NOT_FOUND` khi đọc CSV | Sai `--input-dir`, CSV nằm trong thư mục con lồng nhau | Kiểm tra lại đường dẫn thật bằng `dir`, trỏ đúng thư mục chứa 2 file CSV |
| `HADOOP_HOME and hadoop.home.dir are unset` khi ghi Parquet | Windows cần `winutils.exe`/`hadoop.dll` để Spark thao tác file, biến `HADOOP_HOME` chưa được set | Tải `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x) vào `C:\hadoop\bin`, set biến môi trường `HADOOP_HOME=C:\hadoop`, mở terminal mới để nạp lại biến |
| Biến môi trường không nhận trong terminal VS Code | VS Code chỉ đọc biến môi trường lúc khởi động, không tự refresh khi hệ thống đã set biến mới | Đóng hẳn VS Code và mở lại sau khi set biến |
| Chạy Silver xong không thấy log `[SILVER] ...` nào cả, tưởng job không chạy | Progress bar của Spark (`[Stage ...]`) ghi ký tự điều khiển `\r` liên tục, PowerShell hiển thị trực tiếp bị "nhảy dòng đè" làm mất log | Redirect toàn bộ output ra file (`*> log.txt`) rồi `Get-Content log.txt` để đọc đầy đủ, không bị mất |
| Không xem được nội dung file `.parquet` trực tiếp trong VS Code | Parquet là binary, VS Code không tự đọc được; Marketplace bị chặn không cài được extension xem Parquet | Dùng `pandas.read_parquet()` xuất ra `.html` (`to_html()`), mở bằng trình duyệt (`start file.html`) để xem dạng bảng |

### Ảnh chụp cần bổ sung cho báo cáo cuối

- [ ] Log terminal chạy Bronze thành công
- [ ] Log terminal chạy Silver thành công
- [ ] `dir` kết quả các thư mục Parquet output (bronze + silver)
- [ ] Screenshot bảng dữ liệu Silver xem qua file HTML export

## Kế hoạch tiếp theo

1. Viết `gold_qttg.py`: bung khoảng `TU_THANG`–`DEN_THANG` thành báo cáo
   theo tháng (số người tham gia, số đơn vị, tổng quỹ lương, lương bình
   quân, số người lương 0).
2. Viết `validate_qttg.py`: kiểm tra detail không mồ côi, mỗi người chỉ
   một master ở Silver, Gold không trùng tháng — raise exception nếu lỗi.
3. Dựng Docker Compose (Spark master/worker + Airflow + Postgres) theo
   `HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`, viết DAG nối 4 task theo thứ tự
   Bronze → Silver → Gold → Validate.
4. Chạy toàn bộ pipeline qua Airflow, chụp ảnh Grid/Graph, log từng layer.
