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
| Gold | ✅ Hoàn thành |
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
│           ├── gold_qttg.py         # Da xong
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
│       └── gold/                    # Da co du lieu
│           └── bao_cao_bhxh_thang
└── README.md
```

## Môi trường thực hành

Bronze, Silver, Gold hiện đang được chạy và kiểm chứng trực tiếp trên máy
local (Windows, `python <script>.py`), chưa qua container Spark/Airflow.
Phần Docker Compose + Airflow (theo `HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`) sẽ
được bổ sung ở bước sau.

- OS: Windows
- Python: 3.11.9
- Java: OpenJDK 17 (Microsoft build)
- PySpark: 3.5.3
- Cần `HADOOP_HOME` trỏ tới thư mục chứa `winutils.exe` + `hadoop.dll`
  (Hadoop 3.3.x) để Spark ghi được file trên Windows.
- Cần `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` trỏ **thẳng đường dẫn
  đầy đủ** tới `python.exe` thật (không dựa vào PATH), tránh Windows gọi
  nhầm sang App Execution Alias giả trong `WindowsApps`.

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

Khớp đúng số dòng input và đúng con số kỳ vọng nêu trong tài liệu nghiệp vụ.

## Silver layer

### Việc đã làm

- **Master**: chọn bản ghi mới nhất của mỗi người bằng
  `ROW_NUMBER() OVER (PARTITION BY SO_SO_BHXH ORDER BY CREATED_AT DESC, ID DESC)`,
  giữ `RN = 1`. Thêm cột `IS_DELETED` (mặc định `0`) và `DELETED_AT` (mặc
  định `NULL`) để dự phòng cho soft-delete khi chạy incremental thật sau
  này (batch hiện tại là rebuild toàn bộ bằng overwrite).
- **Detail**: chỉ giữ các dòng có `MASTER_ID` thuộc đúng các master ID mới
  nhất vừa chọn ở trên, đồng thời kiểm tra chéo `NLD_ID` giữa detail và
  master phải khớp nhau.
- **Clean & validate detail**:
  - `trim()` chuẩn hóa `TU_THANG`, `DEN_THANG`.
  - Validate format `YYYYMM` bằng regex `^[0-9]{4}(0[1-9]|1[0-2])$` và
    kiểm tra `TU_THANG <= DEN_THANG`.
  - Cast tường minh `MUC_LUONG` về kiểu số (`DoubleType`).
  - Dòng không hợp lệ tách riêng ra `qttg_bhxh_detail_rejects` kèm
    `REJECT_REASON`, không xóa âm thầm.
- **Kiểm tra sau ghi** (job fail nếu sai): không `SO_SO_BHXH` nào trùng
  > 1 dòng; không detail mồ côi (`left_anti` join theo `MASTER_ID`).

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
  (mỗi người trung bình có ~6.5 phiên bản `CREATED_AT` khác nhau).
- Detail giảm từ 1,000,000 xuống còn **169,658 dòng** (chỉ giữ giai đoạn
  thuộc đúng phiên bản master mới nhất của từng người).
- 0 dòng detail bị loại do sai format tháng.

## Gold layer

### Việc đã làm

- **DIM_THANG**: tự sinh danh sách tháng `YYYYMM` liên tục từ
  `MIN(TU_THANG)` đến `MAX(DEN_THANG)` xuất hiện thực tế trong Silver
  detail — không hard-code khoảng năm.
- **Join tháng với interval**: dùng `broadcast(dim_thang_df)` (DIM_THANG
  chỉ vài trăm dòng) join với detail theo điều kiện
  `THANG_ID BETWEEN TU_THANG AND DEN_THANG` (so sánh string vì đã là
  YYYYMM 6 số cố định) — tránh shuffle nặng khi detail scale lên tới hàng
  triệu dòng.
- **5 chỉ tiêu theo tháng**:
  - `SO_NGUOI_THAM_GIA` = `countDistinct(SO_SO_BHXH)`
  - `SO_DON_VI` = `countDistinct(MA_DON_VI)`
  - `TONG_QUY_LUONG` = `sum(MUC_LUONG)`
  - `LUONG_BINH_QUAN` = trung bình chỉ trên `MUC_LUONG > 0`
  - `SO_NGUOI_LUONG_0` = số người phân biệt có `MUC_LUONG = 0`
- **Không thiếu tháng**: `LEFT JOIN` lại với DIM_THANG đầy đủ, `fillna` 0
  cho các tháng không có ai tham gia, thay vì chỉ xuất hiện tháng có data.
- **Kiểm tra sau ghi** (job fail nếu sai): số dòng Gold khớp đúng số tháng
  trong DIM_THANG; không `THANG_ID` nào trùng dòng; không tháng nào
  `TONG_QUY_LUONG` âm.

### Lệnh chạy

```powershell
cd Datahub
python spark/apps/qttg/gold_qttg.py `
  --silver-dir output/spark_lake/silver `
  --output-dir output/spark_lake/gold
```

### Kết quả chạy thực tế

```text
LAYER=GOLD STATUS=SUCCESS MONTHS=629
```

- Bảng báo cáo trải **629 tháng liên tục**, từ `197402` đến `202606`.
- Tháng đông nhất có 1,753 người tham gia.
- Tổng quỹ lương cộng dồn toàn bộ các tháng ≈ 4,415 tỷ đồng.

## Vấn đề đã gặp và cách xử lý

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| `PATH_NOT_FOUND` khi đọc CSV | Sai `--input-dir`, CSV nằm trong thư mục con lồng nhau | Kiểm tra lại đường dẫn thật bằng `dir`, trỏ đúng thư mục chứa 2 file CSV |
| `HADOOP_HOME and hadoop.home.dir are unset` khi ghi Parquet | Windows cần `winutils.exe`/`hadoop.dll` để Spark thao tác file | Tải `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x) vào `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop`, mở terminal mới |
| Biến môi trường không nhận trong terminal VS Code | VS Code chỉ đọc biến môi trường lúc khởi động, không tự refresh | Đóng hẳn VS Code và mở lại sau mỗi lần set biến môi trường mới |
| Chạy Silver xong không thấy log `[SILVER] ...` nào cả | Progress bar Spark (`[Stage ...]`) ghi ký tự điều khiển `\r` liên tục, PowerShell hiển thị bị "nhảy dòng đè" mất log | Redirect output ra file (`*> log.txt`) rồi `Get-Content log.txt` đọc lại |
| Không xem được nội dung file `.parquet` trong VS Code | Parquet là binary; Marketplace bị chặn không cài được extension xem Parquet | Dùng `pandas.read_parquet()` xuất ra `.html` (`to_html()`), mở bằng trình duyệt (`start file.html`) |
| Gold báo lỗi `Python worker failed to connect back` / `Python was not found` | Windows có App Execution Alias giả (`python.exe` trong `WindowsApps`) bị Spark gọi nhầm khi cần spawn Python worker (xảy ra ở bước `spark.createDataFrame()` tạo DIM_THANG từ list Python — Bronze/Silver không cần bước này nên chưa gặp) | Set `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` trỏ thẳng đường dẫn đầy đủ tới `python.exe` thật, mở terminal mới |

## Ảnh chụp cần bổ sung cho báo cáo cuối

- [ ] Log terminal chạy Bronze / Silver / Gold thành công
- [ ] `dir` kết quả các thư mục Parquet output (bronze + silver + gold)
- [ ] Screenshot bảng dữ liệu Silver/Gold xem qua file HTML export

## Kế hoạch tiếp theo

1. Viết `validate_qttg.py`: kiểm tra detail không mồ côi, mỗi người chỉ
   một master ở Silver, Gold không trùng tháng — raise exception nếu lỗi
   (tổng hợp lại các check đã có rải rác trong từng layer thành 1 bước
   validate độc lập, chạy sau cùng).
2. Dựng Docker Compose (Spark master/worker + Airflow + Postgres) theo
   `HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`, viết DAG nối 4 task theo thứ tự
   Bronze → Silver → Gold → Validate.
3. Chạy toàn bộ pipeline qua Airflow, chụp ảnh Grid/Graph, log từng layer.
