"""
bronze_qttg.py
===============
Bronze layer: ingest nguyen trang 2 file CSV RAW_QTTG_BHXH va RAW_QTTG_BHXH_DETAIL.

- Doc CSV voi schema tuong minh (khong de Spark tu suy kieu).
- Khong loc, khong chon ban ghi moi nhat, khong tinh toan nghiep vu.
- Them 3 cot metadata: source_file, load_time, layer.
- Ghi ra Parquet, sau do doc lai de kiem tra so dong khop voi input.

Chay local (Windows PowerShell vi du):
    spark-submit bronze_qttg.py `
        --input-dir output/raw_qttg_1m `
        --output-dir output/spark_lake/bronze

Chay trong container Spark (theo HUONG_DAN_AIRFLOW_SPARK_LOCAL.md):
    spark-submit bronze_qttg.py \
        --input-dir file:///opt/spark/data/raw_qttg_1m \
        --output-dir file:///opt/spark/data/lake/bronze
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    DoubleType,
)

MASTER_FILE = "RAW_QTTG_BHXH.csv"
DETAIL_FILE = "RAW_QTTG_BHXH_DETAIL.csv"

MASTER_OUT_SUBDIR = "raw_qttg_bhxh"
DETAIL_OUT_SUBDIR = "raw_qttg_bhxh_detail"

EXPECTED_MASTER_ROWS = 142857
EXPECTED_DETAIL_ROWS = 1000000


# ---------------------------------------------------------------------------
# Schema tuong minh, theo dung DDL_RAW_QTTG_BHXH.sql / DDL_RAW_QTTG_BHXH_DETAIL.sql
# CREATED_AT doc tam thoi o dang string roi cast sang timestamp sau, tranh loi
# parse do dinh dang co 6 chu so phan thap phan giay (yyyy-MM-dd HH:mm:ss.SSSSSS).
# ---------------------------------------------------------------------------

MASTER_SCHEMA = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("NLD_ID", LongType(), False),
        StructField("SO_SO_BHXH", StringType(), True),
        StructField("THANG_BD", StringType(), True),
        StructField("THANG_KT", StringType(), True),
        StructField("TT_TG_BHXH", StringType(), True),
        StructField("DT_TG_BHXH", StringType(), True),
        StructField("NAM_TG_BHXH", IntegerType(), True),
        StructField("THANG_TG_BHXH", IntegerType(), True),
        StructField("NAM_TG_BHXH_BB", IntegerType(), True),
        StructField("THANG_TG_BHXH_BB", IntegerType(), True),
        StructField("TT_TG_BHTN", StringType(), True),
        StructField("DT_TG_BHTN", StringType(), True),
        StructField("NAM_TG_BHTN", IntegerType(), True),
        StructField("THANG_TG_BHTN", IntegerType(), True),
        StructField("TT_TG_BHYT", StringType(), True),
        StructField("DT_TG_BHYT", StringType(), True),
        StructField("NAM_TG_BHYT", IntegerType(), True),
        StructField("THANG_TG_BHYT", IntegerType(), True),
        StructField("NAM_NO_BHXH", IntegerType(), True),
        StructField("THANG_NO_BHXH", IntegerType(), True),
        StructField("NAM_NO_BHTN", IntegerType(), True),
        StructField("THANG_NO_BHTN", IntegerType(), True),
        StructField("TT_TG_BH", StringType(), True),
        StructField("DT_TG_BH", StringType(), True),
        StructField("TU_THANG_DVI", StringType(), True),
        StructField("DEN_THANG_DVI", StringType(), True),
        StructField("DEN_THANG_HTTT", StringType(), True),
        StructField("DEN_THANG_BHTN", StringType(), True),
        StructField("THANG_BD_LT", StringType(), True),
        StructField("THANG_KT_LT", StringType(), True),
        StructField("SO_THANG_LT", IntegerType(), True),
        StructField("IS_ERRORS", IntegerType(), True),
        StructField("NGHI_VIEC", IntegerType(), True),
        StructField("IS_CONTINUE", IntegerType(), True),
        StructField("TRUY_DONG", IntegerType(), True),
        StructField("DEN_NGAY", StringType(), True),
        StructField("MA_CD", StringType(), True),
        StructField("MA_NHH", StringType(), True),
        StructField("DD_MA_DON_VI", StringType(), True),
        StructField("DD_THANG_DONG_DEN_XH", StringType(), True),
        StructField("DD_TY_LE_NO_BHXH", DoubleType(), True),
        StructField("DD_THANG_DONG_DEN_YT", StringType(), True),
        StructField("DD_TY_LE_NO_BHYT", DoubleType(), True),
        StructField("DD_THANG_DONG_DEN_TN", StringType(), True),
        StructField("DD_TY_LE_NO_BHTN", DoubleType(), True),
        StructField("DD_THANG_DONG_DEN_TNLD", StringType(), True),
        StructField("DD_TY_LE_NO_TNLD", DoubleType(), True),
        StructField("RAW_RESPONSE", StringType(), True),
        StructField("CREATED_AT", StringType(), True),
    ]
)

DETAIL_SCHEMA = StructType(
    [
        StructField("ID", LongType(), False),
        StructField("MASTER_ID", LongType(), False),
        StructField("NLD_ID", LongType(), False),
        StructField("DOT_PHAT_SINH", StringType(), True),
        StructField("TU_THANG", StringType(), True),
        StructField("DEN_THANG", StringType(), True),
        StructField("MA_DON_VI", StringType(), True),
        StructField("TEN_DON_VI", StringType(), True),
        StructField("LOAI_DT", StringType(), True),
        StructField("LOAI", IntegerType(), True),
        StructField("PA", StringType(), True),
        StructField("DON_VI_TINH", StringType(), True),
        StructField("MA_NT", StringType(), True),
        StructField("CHUC_DANH_CV", StringType(), True),
        StructField("CHUC_DANH_CV_PRE", StringType(), True),
        StructField("NOI_LAM_VIEC", StringType(), True),
        StructField("NOI_DUNG", StringType(), True),
        StructField("MUC_LUONG", DoubleType(), True),
        StructField("MUC_LUONG_TN", DoubleType(), True),
        StructField("MUC_LUONG_BHYT", DoubleType(), True),
        StructField("MUC_LUONG_PC", DoubleType(), True),
        StructField("MUC_LUONG_BS", DoubleType(), True),
        StructField("MUC_LUONG_NLD", DoubleType(), True),
        StructField("MUC_LUONG_NSNN", DoubleType(), True),
        StructField("MUC_LUONG_HS", DoubleType(), True),
        StructField("MUC_LUONG_TT", DoubleType(), True),
        StructField("HS_LUONG", DoubleType(), True),
        StructField("PC_CHUC_VU", DoubleType(), True),
        StructField("PC_THAM_NIEN", DoubleType(), True),
        StructField("PC_NGHE", DoubleType(), True),
        StructField("PC_KHU_VUC", DoubleType(), True),
        StructField("PC_KHAC", DoubleType(), True),
        StructField("PC_TAI_CU", DoubleType(), True),
        StructField("HS_TN", DoubleType(), True),
        StructField("HS_NG", DoubleType(), True),
        StructField("HS_TC", DoubleType(), True),
        StructField("TYLE_BHXH", DoubleType(), True),
        StructField("TYLE_BHYT", DoubleType(), True),
        StructField("TYLE_BHTN", DoubleType(), True),
        StructField("TYLE_TUDV", DoubleType(), True),
        StructField("TYLE_HTTT", DoubleType(), True),
        StructField("TYLE_ODTS", DoubleType(), True),
        StructField("TYLE_TNLD", DoubleType(), True),
        StructField("TYLE_NSNN", DoubleType(), True),
        StructField("DK1", IntegerType(), True),
        StructField("DK2", IntegerType(), True),
        StructField("DK3", IntegerType(), True),
        StructField("DK4", IntegerType(), True),
        StructField("DK5", IntegerType(), True),
        StructField("DK6", IntegerType(), True),
        StructField("IS_BHXH", IntegerType(), True),
        StructField("IS_BHXH_BB", IntegerType(), True),
        StructField("IS_BHTN", IntegerType(), True),
        StructField("IS_BHYT", IntegerType(), True),
        StructField("IS_BHXH2", IntegerType(), True),
        StructField("IS_BHTN2", IntegerType(), True),
        StructField("IS_ERROR", IntegerType(), True),
        StructField("IS_TR", IntegerType(), True),
        StructField("IS_BONUS", IntegerType(), True),
        StructField("ML_TC", IntegerType(), True),
        StructField("GHI_CHU", StringType(), True),
        StructField("KIEM_TRA", IntegerType(), True),
        StructField("SO_THANG", IntegerType(), True),
        StructField("MA_KHOI_TK", StringType(), True),
        StructField("TY_LE_DONG", DoubleType(), True),
        StructField("MUC_DONG", DoubleType(), True),
        StructField("LUONG_CHINH", DoubleType(), True),
        StructField("CHUC_DANH_NLV", StringType(), True),
        StructField("PHUONG_THUC", StringType(), True),
        StructField("PHUONG_THUC_DONG", StringType(), True),
        StructField("MUC_LUONG_PRE", DoubleType(), True),
        StructField("MUC_LUONG_PC_PRE", DoubleType(), True),
        StructField("MUC_LUONG_BS_PRE", DoubleType(), True),
        StructField("HS_LUONG_PRE", DoubleType(), True),
        StructField("PC_CHUC_VU_PRE", DoubleType(), True),
        StructField("PC_THAM_NIEN_PRE", DoubleType(), True),
        StructField("PC_NGHE_PRE", DoubleType(), True),
        StructField("PC_KHU_VUC_PRE", DoubleType(), True),
        StructField("PC_KHAC_PRE", DoubleType(), True),
        StructField("PC_TAI_CU_PRE", DoubleType(), True),
        StructField("LUONG_CHINH_PRE", DoubleType(), True),
        StructField("CREATED_AT", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Bronze ingest QTTG BHXH")
    parser.add_argument(
        "--input-dir",
        default="output/raw_qttg_1m",
        help="Thu muc chua RAW_QTTG_BHXH.csv va RAW_QTTG_BHXH_DETAIL.csv",
    )
    parser.add_argument(
        "--output-dir",
        default="output/spark_lake/bronze",
        help="Thu muc goc de ghi Bronze (se tao 2 thu muc con ben trong)",
    )
    return parser.parse_args()


def read_raw_csv(spark: SparkSession, path: str, schema: StructType):
    return (
        spark.read.option("header", "true")
        .option("sep", ",")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(path)
    )


def add_bronze_metadata(df, source_file: str):
    return (
        df.withColumn("source_file", F.lit(source_file))
        .withColumn("load_time", F.current_timestamp())
        .withColumn("layer", F.lit("BRONZE"))
    )


def write_and_count(spark: SparkSession, df, output_path: str) -> int:
    df.write.mode("overwrite").parquet(output_path)
    # Doc lai tu dia de dam bao so dong dem duoc la so dong da thuc su ghi ra,
    # khong phai so dong cua DataFrame truoc khi ghi (tranh false positive).
    written_df = spark.read.parquet(output_path)
    return written_df.count()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("bronze_qttg")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_dir = args.input_dir.rstrip("/")
    output_dir = args.output_dir.rstrip("/")

    master_input = f"{input_dir}/{MASTER_FILE}"
    detail_input = f"{input_dir}/{DETAIL_FILE}"
    master_output = f"{output_dir}/{MASTER_OUT_SUBDIR}"
    detail_output = f"{output_dir}/{DETAIL_OUT_SUBDIR}"

    print(f"[BRONZE] Doc master tu: {master_input}")
    master_raw_df = read_raw_csv(spark, master_input, MASTER_SCHEMA)
    master_input_rows = master_raw_df.count()
    master_bronze_df = add_bronze_metadata(master_raw_df, MASTER_FILE)

    print(f"[BRONZE] Doc detail tu: {detail_input}")
    detail_raw_df = read_raw_csv(spark, detail_input, DETAIL_SCHEMA)
    detail_input_rows = detail_raw_df.count()
    detail_bronze_df = add_bronze_metadata(detail_raw_df, DETAIL_FILE)

    print(f"[BRONZE] Ghi master ra: {master_output}")
    master_written_rows = write_and_count(spark, master_bronze_df, master_output)

    print(f"[BRONZE] Ghi detail ra: {detail_output}")
    detail_written_rows = write_and_count(spark, detail_bronze_df, detail_output)

    # ---- Kiem tra so dong ----
    errors = []

    if master_written_rows != master_input_rows:
        errors.append(
            f"MASTER: input={master_input_rows} nhung ghi ra={master_written_rows}"
        )
    if master_written_rows != EXPECTED_MASTER_ROWS:
        errors.append(
            f"MASTER: ky vong {EXPECTED_MASTER_ROWS} dong nhung co {master_written_rows}"
        )

    if detail_written_rows != detail_input_rows:
        errors.append(
            f"DETAIL: input={detail_input_rows} nhung ghi ra={detail_written_rows}"
        )
    if detail_written_rows != EXPECTED_DETAIL_ROWS:
        errors.append(
            f"DETAIL: ky vong {EXPECTED_DETAIL_ROWS} dong nhung co {detail_written_rows}"
        )

    if errors:
        for e in errors:
            print(f"[BRONZE] LOI: {e}")
        print(
            f"LAYER=BRONZE STATUS=FAILED MASTER_ROWS={master_written_rows} "
            f"DETAIL_ROWS={detail_written_rows}"
        )
        spark.stop()
        sys.exit(1)

    print(
        f"LAYER=BRONZE STATUS=SUCCESS MASTER_ROWS={master_written_rows} "
        f"DETAIL_ROWS={detail_written_rows}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
