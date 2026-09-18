# QTTG BHXH — Bronze → Silver → Gold → Validate (Spark + Airflow local)

Bài thực hành xử lý 2 bảng `RAW_QTTG_BHXH` (master) và `RAW_QTTG_BHXH_DETAIL`
(detail) theo mô hình lakehouse Bronze/Silver/Gold, chạy Spark local trên
Docker Desktop (Windows), điều phối bằng Airflow.

Tài liệu nghiệp vụ gốc: `PHAN_TICH_NGHIEP_VU_QTTG_BHXH.md`
Hướng dẫn Airflow + Spark local: `HUONG_DAN_AIRFLOW_SPARK_LOCAL.md`

## Tổng kết tuần

| Layer | Trạng thái | Job chạy OK? |
|---|---|---|
| Bronze | ✅ Hoàn thành | ✅ Chạy trực tiếp trên Windows + chạy qua Airflow (Docker), đều verify |
| Silver | ✅ Hoàn thành | ✅ Chạy trực tiếp trên Windows + chạy qua Airflow (Docker), đều verify |
| Gold | ✅ Hoàn thành | ✅ Chạy trực tiếp trên Windows + chạy qua Airflow (Docker), đều verify |
| Validate | ✅ Hoàn thành | ✅ Chạy trực tiếp trên Windows + chạy qua Airflow (Docker), đều verify |
| Airflow DAG | ✅ Hoàn thành | ✅ Đã trigger thật trên Airflow UI, cả 4 task chuyển `success`, đúng dependency |

**Output nằm ở đâu** (tất cả dưới `Datahub/output/spark_lake/`):

| Layer | Đường dẫn | Nội dung |
|---|---|---|
| Bronze | `bronze/raw_qttg_bhxh`, `bronze/raw_qttg_bhxh_detail` | CSV gốc + metadata, chưa xử lý nghiệp vụ |
| Silver | `silver/qttg_bhxh`, `silver/qttg_bhxh_detail` | Đã dedupe theo người, detail đã lọc theo master mới nhất |
| Silver (reject) | `silver/qttg_bhxh_detail_rejects` | Detail sai format tháng (hiện tại: 0 dòng) |
| Gold | `gold/bao_cao_bhxh_thang` | Báo cáo tổng hợp theo tháng |

**Count / số liệu kiểm tra** (chạy trên bộ CSV gốc 142,857 dòng master / 1,000,000 dòng detail, kết quả giống nhau ở cả 2 cách chạy — trực tiếp trên Windows và qua Airflow/Docker):

```text
LAYER=BRONZE   STATUS=SUCCESS MASTER_ROWS=142857 DETAIL_ROWS=1000000
LAYER=SILVER   STATUS=SUCCESS MASTER_ROWS=21838  DETAIL_ROWS=169658  DETAIL_REJECTED=0
LAYER=GOLD     STATUS=SUCCESS MONTHS=629
LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0
```

DAG `qttg_bronze_silver_gold` trên Airflow UI, cả 4 task đều `success`:
<img width="1898" height="869" alt="image" src="https://github.com/user-attachments/assets/243cfa51-ea19-4629-a90d-c6c48786f45c" />


```text
bronze_qttg (success) → silver_qttg (success) → gold_qttg (success) → validate_qttg (success)
```

**Lỗi / vướng mắc trong tuần** — xem bảng chi tiết ở mục "Vấn đề đã gặp và
cách xử lý" bên dưới. Tóm tắt: giai đoạn chạy trực tiếp trên Windows gặp
nhiều vướng mắc môi trường (thiếu `winutils.exe`/`HADOOP_HOME`, App
Execution Alias giả của Python, ổ đĩa hết dung lượng, Docker Desktop hỏng
cấu hình phải factory reset); giai đoạn Airflow gặp 2 vướng mắc hạ tầng
riêng (build Dockerfile treo do prompt tương tác của `apt-get`, và image
`bitnami/spark:3.5` không còn tồn tại công khai do Bitnami đổi chính sách
catalog). Không có vướng mắc nào thuộc về logic ETL — tất cả lỗi đều ở tầng
môi trường/hạ tầng, cách xử lý đã ghi lại đầy đủ để tránh lặp lại.

## Cấu trúc thư mục

```text
Datahub/
├── docker-compose.yml                # Spark Master/Worker + Airflow + Postgres
├── export_gold_html.py               # Tien ich xem nhanh Gold dang bang HTML
├── spark/
│   └── apps/
│       └── qttg/
│           ├── bronze_qttg.py        # Da xong, da chay that (Windows + Airflow)
│           ├── silver_qttg.py        # Da xong, da chay that (Windows + Airflow)
│           ├── gold_qttg.py          # Da xong, da chay that (Windows + Airflow)
│           └── validate_qttg.py      # Da xong, da chay that (Windows + Airflow)
├── airflow-local/
│   ├── Dockerfile                    # Airflow + Docker CLI
│   ├── dags/
│   │   └── dag_qttg_bronze_silver_gold.py   # Da trigger thanh cong tren UI
│   └── logs/
├── output/
│   ├── raw_qttg_1m/                  # 2 CSV nguon
│   │   ├── RAW_QTTG_BHXH.csv
│   │   └── RAW_QTTG_BHXH_DETAIL.csv
│   └── spark_lake/
│       ├── bronze/
│       ├── silver/
│       └── gold/
└── README.md
```

## Môi trường thực hành

Đã chạy thành công theo **cả hai cách**:

1. **Trực tiếp trên Windows** (`python <script>.py`) — dùng để phát triển
   và test nhanh từng layer.
2. **Qua Airflow, trong Docker Desktop** (`spark-submit` bên trong
   container Spark Master, do Airflow điều phối) — dùng để mô phỏng đúng
   pipeline production.

- OS: Windows, Docker Desktop
- Python: 3.11.9 (chạy trực tiếp), PySpark 3.5.3
- Java: OpenJDK 17 (Microsoft build)
- Spark trong Docker: image chính thức `apache/spark:3.5.3`
- Airflow: `apache/airflow:2.8.1-python3.10` + Docker CLI, LocalExecutor, Postgres 15

Khi chạy trực tiếp trên Windows cần:
- `HADOOP_HOME` trỏ tới thư mục chứa `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x).
- `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` trỏ thẳng đường dẫn đầy đủ tới `python.exe` thật.

> Khi chạy qua Docker (Linux container), **không cần** `HADOOP_HOME`,
> `winutils.exe`, hay set `PYSPARK_PYTHON` — các vướng mắc đó chỉ phát sinh
> khi chạy PySpark trực tiếp trên Windows, và thực tế biến mất hoàn toàn khi
> chuyển sang chạy qua Airflow/Docker.

## Bronze layer

- Đọc nguyên trạng 2 CSV với schema tường minh (khớp đúng
  `DDL_RAW_QTTG_BHXH.sql` / `DDL_RAW_QTTG_BHXH_DETAIL.sql`).
- Thêm 3 cột metadata: `source_file`, `load_time`, `layer = "BRONZE"`.
- Kiểm tra số dòng sau khi ghi khớp input và khớp con số kỳ vọng.

```powershell
python spark/apps/qttg/bronze_qttg.py `
  --input-dir output/raw_qttg_1m `
  --output-dir output/spark_lake/bronze
```

```text
LAYER=BRONZE STATUS=SUCCESS MASTER_ROWS=142857 DETAIL_ROWS=1000000
```

## Silver layer

- **Master**: `ROW_NUMBER() OVER (PARTITION BY SO_SO_BHXH ORDER BY CREATED_AT DESC, ID DESC)`,
  giữ `RN = 1`. Thêm `IS_DELETED=0`, `DELETED_AT=NULL` (dự phòng soft-delete).
- **Detail**: chỉ giữ theo `MASTER_ID` thuộc master mới nhất, kiểm tra chéo `NLD_ID`.
- **Clean & validate**: `trim()` tháng, regex `YYYYMM`, `TU_THANG <= DEN_THANG`,
  cast `MUC_LUONG` sang `DoubleType`. Dòng lỗi tách riêng ra
  `qttg_bhxh_detail_rejects` kèm `REJECT_REASON`.
- **Kiểm tra sau ghi**: không `SO_SO_BHXH` trùng > 1 dòng; không detail mồ côi.

```powershell
python spark/apps/qttg/silver_qttg.py `
  --bronze-dir output/spark_lake/bronze `
  --output-dir output/spark_lake/silver
```

```text
LAYER=SILVER STATUS=SUCCESS MASTER_ROWS=21838 DETAIL_ROWS=169658 DETAIL_REJECTED=0
```

## Gold layer

- **DIM_THANG**: tự sinh từ `MIN(TU_THANG)` đến `MAX(DEN_THANG)` thực tế trong Silver.
- **Join tháng với interval**: `broadcast(dim_thang_df)` join điều kiện
  `THANG_ID BETWEEN TU_THANG AND DEN_THANG` — tránh shuffle nặng ở quy mô 1 triệu dòng.
- **5 chỉ tiêu**: `SO_NGUOI_THAM_GIA`, `SO_DON_VI`, `TONG_QUY_LUONG`,
  `LUONG_BINH_QUAN` (chỉ trên lương > 0), `SO_NGUOI_LUONG_0`.
- **Không thiếu tháng**: `LEFT JOIN` lại DIM_THANG đầy đủ, `fillna` 0.
- **Kiểm tra sau ghi**: số dòng khớp DIM_THANG; không trùng `THANG_ID`; không quỹ lương âm.

```powershell
python spark/apps/qttg/gold_qttg.py `
  --silver-dir output/spark_lake/silver `
  --output-dir output/spark_lake/gold
```

```text
LAYER=GOLD STATUS=SUCCESS MONTHS=629
```

Xem nhanh dạng bảng dễ đọc:

```powershell
python export_gold_html.py --gold-dir output/spark_lake/gold --out bao_cao_bhxh_thang.html
start bao_cao_bhxh_thang.html
```

## Validate layer

Đọc lại Silver + Gold **từ đĩa** (độc lập với code Silver/Gold, để vẫn bắt
được lỗi nếu sau này code các layer khác thay đổi mà quên cập nhật check):

1. Silver detail không mồ côi (`MASTER_ID` phải thuộc Silver master).
2. Mỗi `SO_SO_BHXH` chỉ còn đúng 1 dòng master tại Silver.
3. `TU_THANG <= DEN_THANG` cho toàn bộ Silver detail.
4. Gold không trùng `THANG_ID`.

Nếu bất kỳ kiểm tra nào thất bại, script `raise RuntimeError` (exit code
khác 0) — đúng yêu cầu "task xanh nhưng dữ liệu sai phải bị chặn lại".

```powershell
python spark/apps/qttg/validate_qttg.py --lake-dir output/spark_lake
```

```text
LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0
```

## Airflow — điều phối Bronze → Silver → Gold → Validate

### Kiến trúc

```text
Windows + Docker Desktop
│
├── airflow-webserver     http://localhost:8082
├── airflow-scheduler
├── postgres              metadata Airflow
├── spark-master          http://localhost:8080
└── spark-worker          http://localhost:8081
```

Airflow không chạy logic ETL trực tiếp — mỗi task chỉ `docker exec` vào
container Spark Master rồi `spark-submit` file PySpark đã có sẵn:

```text
BashOperator → Docker socket → docker exec <spark-master> → spark-submit
```

### DAG

File: `airflow-local/dags/dag_qttg_bronze_silver_gold.py`, `dag_id`:
`qttg_bronze_silver_gold`, 4 task nối tiếp:

```text
bronze_qttg >> silver_qttg >> gold_qttg >> validate_qttg
```

| Task | Lệnh spark-submit gọi tới | Tham số |
|---|---|---|
| `bronze_qttg` | `bronze_qttg.py` | `--input-dir file:///opt/spark/data/raw_qttg_1m --output-dir file:///opt/spark/data/lake/bronze` |
| `silver_qttg` | `silver_qttg.py` | `--bronze-dir file:///opt/spark/data/lake/bronze --output-dir file:///opt/spark/data/lake/silver` |
| `gold_qttg` | `gold_qttg.py` | `--silver-dir file:///opt/spark/data/lake/silver --output-dir file:///opt/spark/data/lake/gold` |
| `validate_qttg` | `validate_qttg.py` | `--lake-dir file:///opt/spark/data/lake` |

### Cách dựng và chạy

```powershell
cd Datahub
$env:AIRFLOW_UID = "50000"

docker compose build airflow-init airflow-webserver airflow-scheduler
docker compose up airflow-init
docker compose up -d postgres spark-master spark-worker airflow-webserver airflow-scheduler
docker compose ps
```

Xác nhận Worker đã đăng ký vào Master:

```powershell
docker logs qttg-spark-master --tail 20
```

Mở `http://localhost:8082`, đăng nhập `admin` / `admin`, tìm DAG
`qttg_bronze_silver_gold`, bật nếu đang paused, chọn **Trigger DAG**.

### Kết quả chạy thật trên Airflow UI

Đã trigger DAG `qttg_bronze_silver_gold` thành công — cả 4 task đều
chuyển `success` theo đúng thứ tự:

```text
bronze_qttg (success) → silver_qttg (success) → gold_qttg (success) → validate_qttg (success)
```

Log thật của task `validate_qttg` (đọc trực tiếp từ
`airflow-local/logs/dag_id=qttg_bronze_silver_gold/run_id=.../task_id=validate_qttg/`):

```text
LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0
```

Trùng khớp hoàn toàn với kết quả chạy trực tiếp trên Windows — xác nhận
pipeline cho ra cùng một kết quả bất kể chạy bằng cách nào.

## Vấn đề đã gặp và cách xử lý

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| `PATH_NOT_FOUND` khi đọc CSV | Sai `--input-dir`, CSV nằm trong thư mục con lồng nhau | Kiểm tra đường dẫn thật bằng `dir`, trỏ đúng thư mục chứa 2 file CSV |
| `HADOOP_HOME and hadoop.home.dir are unset` khi ghi Parquet | Windows cần `winutils.exe`/`hadoop.dll` để Spark thao tác file | Tải `winutils.exe` + `hadoop.dll` (Hadoop 3.3.x) vào `C:\hadoop\bin`, set `HADOOP_HOME=C:\hadoop`, mở terminal mới |
| Biến môi trường không nhận trong terminal VS Code | VS Code chỉ đọc biến môi trường lúc khởi động, không tự refresh | Đóng hẳn VS Code và mở lại sau mỗi lần set biến môi trường mới |
| Chạy Silver xong không thấy log `[SILVER] ...` nào cả | Progress bar Spark (`[Stage ...]`) ghi ký tự điều khiển `\r` liên tục, PowerShell hiển thị bị "nhảy dòng đè" mất log | Redirect output ra file (`*> log.txt`) rồi `Get-Content log.txt` đọc lại |
| Không xem được nội dung file `.parquet` trong VS Code | Parquet là binary; Marketplace bị chặn không cài được extension xem Parquet | Dùng `pandas.read_parquet()` xuất ra `.html`, mở bằng trình duyệt |
| Gold báo lỗi `Python worker failed to connect back` / `Python was not found` | Windows có App Execution Alias giả (`python.exe` trong `WindowsApps`) bị Spark gọi nhầm khi cần spawn Python worker | Set `PYSPARK_PYTHON`, `PYSPARK_DRIVER_PYTHON` trỏ thẳng đường dẫn đầy đủ tới `python.exe` thật |
| Docker Desktop lỗi `daemon.json is invalid`, `read-only file system`, `500 Internal Server Error` khi build | Cấu hình/ổ đĩa ảo nội bộ của Docker Desktop bị hỏng (không liên quan tới code) | Reset dockerd configuration → `wsl --shutdown` → restart toàn bộ Windows → nếu vẫn lỗi, unregister + factory reset Docker Desktop |
| Build Airflow image treo, lỗi `dpkg: error processing package perl` | `apt-get install docker.io` kéo theo `perl` cần xác nhận tương tác file cấu hình, môi trường build không tương tác được | Thêm `ENV DEBIAN_FRONTEND=noninteractive` và `-o Dpkg::Options::="--force-confdef/--force-confold"` vào Dockerfile |
| `exec /usr/bin/dumb-init: exec format error` khi chạy `airflow-init` | Image nền bị cache/pull nhầm kiến trúc CPU (thường do Docker Desktop vừa gặp sự cố) | Ép `platform: linux/amd64` trên mọi service trong `docker-compose.yml`, xóa cache và pull lại |
| `docker.io/bitnami/spark:3.5: not found` khi kéo image Spark | Bitnami ngừng cung cấp tag phiên bản miễn phí từ cuối 2025, chuyển sang catalog thương mại "Bitnami Secure Images" | Đổi sang image chính thức `apache/spark:3.5.3` (do Apache Software Foundation duy trì), dùng `spark-class` trực tiếp thay cho biến `SPARK_MODE` của Bitnami |
| Ổ đĩa C: hết dung lượng (0 byte trống) | Nhiều bản zip/CSV trùng lặp trong Downloads + ổ đĩa ảo Docker phình to sau nhiều lần build/factory-reset | Dọn Recycle Bin, xóa project/zip cũ trùng lặp, kiểm tra dung lượng bằng `Get-PSDrive` |

## Ảnh chụp đã có cho báo cáo cuối

- [x] Log terminal chạy Bronze / Silver / Gold / Validate thành công (Windows)
- [x] `docker compose ps` — toàn bộ container `running`/`healthy`
- [x] `docker logs qttg-spark-master` — Worker đã đăng ký vào Master
- [x] Airflow Graph — cả 4 task `bronze_qttg → silver_qttg → gold_qttg → validate_qttg` màu xanh (`success`)
- [x] Log task `validate_qttg` trên Airflow — `LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0`
- [ ] Bảng dữ liệu Gold xem qua `bao_cao_bhxh_thang.html` (tùy chọn, để minh họa báo cáo)
