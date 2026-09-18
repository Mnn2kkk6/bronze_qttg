"""
validate_qttg.py
==================
Buoc validate doc lap, chay sau Gold trong pipeline Airflow: doc lai
Silver + Gold da ghi tren dia va kiem tra tinh toan ven - khong phu
thuoc vao code cua Silver/Gold (thiet ke de van bat duoc loi neu sau
nay code Silver/Gold thay doi ma quen cap nhat check ben trong).

Kiem tra:
1. Silver detail khong mo coi (MASTER_ID phai thuoc Silver master).
2. Moi SO_SO_BHXH chi con dung 1 dong master tai Silver.
3. TU_THANG <= DEN_THANG cho toan bo Silver detail.
4. Gold khong trung THANG_ID.

Neu bat ky kiem tra nao that bai, script raise RuntimeError (exit code
khac 0) de Airflow danh dau task that bai - tranh tinh trang "task xanh
nhung du lieu sai".

Chay:
    python validate_qttg.py --lake-dir output/spark_lake
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

MASTER_SILVER_SUBDIR = "silver/qttg_bhxh"
DETAIL_SILVER_SUBDIR = "silver/qttg_bhxh_detail"
GOLD_SUBDIR = "gold/bao_cao_bhxh_thang"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate ket qua Silver/Gold QTTG BHXH")
    parser.add_argument(
        "--lake-dir",
        default="output/spark_lake",
        help="Thu muc goc chua silver/ va gold/ (vi du output/spark_lake)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    lake_dir = args.lake_dir.rstrip("/")

    master_path = f"{lake_dir}/{MASTER_SILVER_SUBDIR}"
    detail_path = f"{lake_dir}/{DETAIL_SILVER_SUBDIR}"
    gold_path = f"{lake_dir}/{GOLD_SUBDIR}"

    spark = (
        SparkSession.builder.appName("validate_qttg")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[VALIDATE] Doc Silver master tu: {master_path}")
    master_df = spark.read.parquet(master_path)

    print(f"[VALIDATE] Doc Silver detail tu: {detail_path}")
    detail_df = spark.read.parquet(detail_path)

    print(f"[VALIDATE] Doc Gold tu: {gold_path}")
    gold_df = spark.read.parquet(gold_path)

    errors = []

    # 1) Detail khong mo coi: MASTER_ID phai thuoc Silver master
    valid_master_ids_df = master_df.select(F.col("ID").alias("VALID_ID"))
    orphan_count = detail_df.join(
        valid_master_ids_df,
        detail_df["MASTER_ID"] == valid_master_ids_df["VALID_ID"],
        "left_anti",
    ).count()
    if orphan_count > 0:
        errors.append(
            f"Co {orphan_count} dong Silver detail mo coi (MASTER_ID khong khop Silver master)"
        )

    # 2) Moi nguoi chi con dung 1 dong master tai Silver
    duplicate_persons = (
        master_df.groupBy("SO_SO_BHXH").count().filter(F.col("count") > 1).count()
    )
    if duplicate_persons > 0:
        errors.append(f"Co {duplicate_persons} SO_SO_BHXH bi trung > 1 dong master tai Silver")

    # 3) TU_THANG <= DEN_THANG cho toan bo Silver detail
    invalid_month_order_count = detail_df.filter(
        F.col("TU_THANG") > F.col("DEN_THANG")
    ).count()
    if invalid_month_order_count > 0:
        errors.append(
            f"Co {invalid_month_order_count} dong Silver detail co TU_THANG > DEN_THANG"
        )

    # 4) Gold khong trung thang
    duplicate_months = (
        gold_df.groupBy("THANG_ID").count().filter(F.col("count") > 1).count()
    )
    if duplicate_months > 0:
        errors.append(f"Co {duplicate_months} THANG_ID bi trung dong trong Gold")

    error_rows = (
        orphan_count + duplicate_persons + invalid_month_order_count + duplicate_months
    )

    if errors:
        for e in errors:
            print(f"[VALIDATE] LOI: {e}")
        print(f"LAYER=VALIDATE STATUS=FAILED ERROR_ROWS={error_rows}")
        spark.stop()
        # Raise de Airflow (va bat ky nguoi goi script nao khac) nhan duoc
        # exit code khac 0 - khong duoc phep "task xanh nhung du lieu sai".
        raise RuntimeError("Validate that bai: " + "; ".join(errors))

    print(
        f"[VALIDATE] Silver: {master_df.count()} nguoi, {detail_df.count()} dong detail "
        f"| Gold: {gold_df.count()} thang"
    )
    print("LAYER=VALIDATE STATUS=SUCCESS ERROR_ROWS=0")
    spark.stop()


if __name__ == "__main__":
    main()
