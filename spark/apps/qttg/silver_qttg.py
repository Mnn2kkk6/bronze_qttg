"""
silver_qttg.py
================
Silver layer: chuan hoa du lieu tu Bronze.

- Master: moi SO_SO_BHXH chi giu 1 dong moi nhat
  (PARTITION BY SO_SO_BHXH ORDER BY CREATED_AT DESC, ID DESC, RN = 1).
- Detail: chi giu detail co MASTER_ID thuoc cac master ID moi nhat da chon
  o buoc tren (khong lay toan bo detail Bronze).
- Clean detail: chuan hoa TU_THANG/DEN_THANG, kiem tra dung format YYYYMM,
  kiem tra TU_THANG <= DEN_THANG, cast MUC_LUONG ve kieu so. Record loi
  duoc tach rieng ra, khong am tham xoa.
- Ghi output ra output/spark_lake/silver/qttg_bhxh va qttg_bhxh_detail.
- Kiem tra: moi nguoi dung 1 dong master, detail khong mo coi (MASTER_ID
  phai thuoc master Silver, NLD_ID phai khop giua master va detail).

Chay:
    python silver_qttg.py \
        --bronze-dir output/spark_lake/bronze \
        --output-dir output/spark_lake/silver
"""

import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

MASTER_BRONZE_SUBDIR = "raw_qttg_bhxh"
DETAIL_BRONZE_SUBDIR = "raw_qttg_bhxh_detail"

MASTER_SILVER_SUBDIR = "qttg_bhxh"
DETAIL_SILVER_SUBDIR = "qttg_bhxh_detail"
DETAIL_REJECTS_SUBDIR = "qttg_bhxh_detail_rejects"

# YYYYMM: 4 chu so nam + 2 chu so thang (01-12)
MONTH_REGEX = r"^[0-9]{4}(0[1-9]|1[0-2])$"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Silver chuan hoa QTTG BHXH")
    parser.add_argument(
        "--bronze-dir",
        default="output/spark_lake/bronze",
        help="Thu muc goc chua Bronze (co 2 thu muc con raw_qttg_bhxh, raw_qttg_bhxh_detail)",
    )
    parser.add_argument(
        "--output-dir",
        default="output/spark_lake/silver",
        help="Thu muc goc de ghi Silver",
    )
    return parser.parse_args()


def build_master_silver(spark: SparkSession, bronze_master_path: str):
    """Chon ban ghi moi nhat cua moi nguoi theo SO_SO_BHXH."""
    master_raw_df = spark.read.parquet(bronze_master_path)

    window_latest = Window.partitionBy("SO_SO_BHXH").orderBy(
        F.col("CREATED_AT").desc(), F.col("ID").desc()
    )

    master_silver_df = (
        master_raw_df.withColumn("RN", F.row_number().over(window_latest))
        .filter(F.col("RN") == 1)
        .drop("RN")
        # Soft-delete placeholder: batch nay la rebuild toan bo (overwrite),
        # chua lam merge/upsert so voi Silver lan truoc nen luon la active.
        # Neu sau nay chay incremental thi phai UPDATE cac dong bi thay the
        # thanh IS_DELETED=1, DELETED_AT=now() thay vi overwrite toan bo.
        .withColumn("IS_DELETED", F.lit(0))
        .withColumn("DELETED_AT", F.lit(None).cast("timestamp"))
    )
    return master_raw_df, master_silver_df


def build_detail_silver(spark: SparkSession, bronze_detail_path: str, master_silver_df):
    """Chi giu detail thuoc cac master ID moi nhat, sau do clean va tach loi."""
    detail_raw_df = spark.read.parquet(bronze_detail_path)

    latest_master_ids_df = master_silver_df.select(
        F.col("ID").alias("_LATEST_MASTER_ID"),
        F.col("NLD_ID").alias("_LATEST_MASTER_NLD_ID"),
    )

    # Chi giu detail co MASTER_ID nam trong danh sach master moi nhat,
    # dong thoi kiem tra NLD_ID cua detail phai khop voi NLD_ID cua master
    # (bao ve khoi lien ket sai du lieu).
    detail_scoped_df = detail_raw_df.join(
        latest_master_ids_df,
        detail_raw_df["MASTER_ID"] == latest_master_ids_df["_LATEST_MASTER_ID"],
        "inner",
    ).filter(F.col("NLD_ID") == F.col("_LATEST_MASTER_NLD_ID")).drop(
        "_LATEST_MASTER_ID", "_LATEST_MASTER_NLD_ID"
    )

    # ---- Clean: chuan hoa TU_THANG / DEN_THANG ----
    detail_clean_df = detail_scoped_df.withColumn(
        "TU_THANG", F.trim(F.col("TU_THANG"))
    ).withColumn("DEN_THANG", F.trim(F.col("DEN_THANG")))

    # Cast MUC_LUONG ve kieu so (phong truong hop Bronze con giu string o
    # nguon khac; voi CSV hien tai da la DoubleType tu Bronze nen day la
    # buoc bao ve them, khong doi gia tri).
    detail_clean_df = detail_clean_df.withColumn(
        "MUC_LUONG", F.col("MUC_LUONG").cast(DoubleType())
    )

    # ---- Validate format thang: dung 6 so, thang 01-12, TU_THANG <= DEN_THANG ----
    is_valid_month = (
        F.col("TU_THANG").rlike(MONTH_REGEX)
        & F.col("DEN_THANG").rlike(MONTH_REGEX)
        & (F.col("TU_THANG") <= F.col("DEN_THANG"))
    )

    detail_valid_df = detail_clean_df.filter(is_valid_month)
    detail_reject_df = detail_clean_df.filter(~is_valid_month).withColumn(
        "REJECT_REASON",
        F.when(~F.col("TU_THANG").rlike(MONTH_REGEX), F.lit("TU_THANG sai format YYYYMM"))
        .when(~F.col("DEN_THANG").rlike(MONTH_REGEX), F.lit("DEN_THANG sai format YYYYMM"))
        .otherwise(F.lit("TU_THANG > DEN_THANG")),
    )

    return detail_raw_df, detail_scoped_df, detail_valid_df, detail_reject_df


def write_and_count(spark: SparkSession, df, output_path: str) -> int:
    df.write.mode("overwrite").parquet(output_path)
    written_df = spark.read.parquet(output_path)
    return written_df.count()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("silver_qttg")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    bronze_dir = args.bronze_dir.rstrip("/")
    output_dir = args.output_dir.rstrip("/")

    bronze_master_path = f"{bronze_dir}/{MASTER_BRONZE_SUBDIR}"
    bronze_detail_path = f"{bronze_dir}/{DETAIL_BRONZE_SUBDIR}"
    master_output = f"{output_dir}/{MASTER_SILVER_SUBDIR}"
    detail_output = f"{output_dir}/{DETAIL_SILVER_SUBDIR}"
    detail_rejects_output = f"{output_dir}/{DETAIL_REJECTS_SUBDIR}"

    print(f"[SILVER] Doc Bronze master tu: {bronze_master_path}")
    master_raw_df, master_silver_df = build_master_silver(spark, bronze_master_path)
    master_raw_rows = master_raw_df.count()
    distinct_persons = master_raw_df.select("SO_SO_BHXH").distinct().count()

    print(f"[SILVER] Doc Bronze detail tu: {bronze_detail_path}")
    detail_raw_df, detail_scoped_df, detail_valid_df, detail_reject_df = build_detail_silver(
        spark, bronze_detail_path, master_silver_df
    )
    detail_raw_rows = detail_raw_df.count()

    print(f"[SILVER] Ghi master ra: {master_output}")
    master_written_rows = write_and_count(spark, master_silver_df, master_output)

    print(f"[SILVER] Ghi detail hop le ra: {detail_output}")
    detail_written_rows = write_and_count(spark, detail_valid_df, detail_output)

    reject_count = detail_reject_df.count()
    if reject_count > 0:
        print(f"[SILVER] Ghi detail loi ({reject_count} dong) ra: {detail_rejects_output}")
        write_and_count(spark, detail_reject_df, detail_rejects_output)
    else:
        print("[SILVER] Khong co detail loi format thang.")

    # ---- Kiem tra ----
    errors = []

    # 1) Moi SO_SO_BHXH dung 1 dong Silver
    duplicate_persons = (
        master_silver_df.groupBy("SO_SO_BHXH").count().filter(F.col("count") > 1).count()
    )
    if duplicate_persons > 0:
        errors.append(f"MASTER: co {duplicate_persons} SO_SO_BHXH bi trung > 1 dong Silver")

    if master_written_rows != distinct_persons:
        errors.append(
            f"MASTER: so dong Silver ({master_written_rows}) khac so nguoi phan biet "
            f"trong Bronze ({distinct_persons})"
        )

    # 2) Detail khong mo coi: MASTER_ID phai thuoc master Silver
    valid_master_ids_df = master_silver_df.select(F.col("ID").alias("VALID_ID"))
    orphan_detail_count = (
        detail_valid_df.join(
            valid_master_ids_df,
            detail_valid_df["MASTER_ID"] == valid_master_ids_df["VALID_ID"],
            "left_anti",
        ).count()
    )
    if orphan_detail_count > 0:
        errors.append(f"DETAIL: co {orphan_detail_count} dong mo coi (MASTER_ID khong khop Silver)")

    if errors:
        for e in errors:
            print(f"[SILVER] LOI: {e}")
        print(
            f"LAYER=SILVER STATUS=FAILED MASTER_ROWS={master_written_rows} "
            f"DETAIL_ROWS={detail_written_rows} DETAIL_REJECTED={reject_count}"
        )
        spark.stop()
        sys.exit(1)

    print(
        f"[SILVER] Bronze: {master_raw_rows} dong master / {distinct_persons} nguoi phan biet "
        f"/ {detail_raw_rows} dong detail"
    )
    print(
        f"[SILVER] Detail sau khi loc theo master moi nhat: {detail_scoped_df.count()} dong, "
        f"trong do hop le {detail_written_rows}, loi format thang {reject_count}"
    )
    print(
        f"LAYER=SILVER STATUS=SUCCESS MASTER_ROWS={master_written_rows} "
        f"DETAIL_ROWS={detail_written_rows} DETAIL_REJECTED={reject_count}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
