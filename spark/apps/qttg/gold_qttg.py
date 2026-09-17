"""
gold_qttg.py
=============
Gold layer: bao cao tong hop tham gia BHXH theo tung thang.

- Doc master + detail tu Silver (khong doc lai Bronze).
- Tao DIM_THANG: danh sach tat ca cac thang YYYYMM tu THANG nho nhat
  (TU_THANG) den THANG lon nhat (DEN_THANG) xuat hien trong Silver detail -
  dam bao bao cao la mot chuoi thang lien tuc, khong bi "thieu thang"
  khi thang do khong co ai tham gia.
- Join thang voi khoang TU_THANG -> DEN_THANG cua tung detail (dieu kien
  THANG_ID BETWEEN TU_THANG AND DEN_THANG, so sanh string vi da la
  YYYYMM 6 so co dinh nen so sanh lexicographic == so sanh so).
- Tinh 5 chi tieu: SO_NGUOI_THAM_GIA, SO_DON_VI, TONG_QUY_LUONG,
  LUONG_BINH_QUAN (chi tinh tren MUC_LUONG > 0), SO_NGUOI_LUONG_0.
- Ghi output ra output/spark_lake/gold/bao_cao_bhxh_thang.

Luu y ve quy mo: Gold doc tu Silver (da giam tu 1,000,000 dong detail
Bronze xuong con so dong thuc te sau khi chon master moi nhat), nhung
range-join voi DIM_THANG duoc broadcast nen van xu ly on dinh o quy mo
detail toi 1 trieu dong.

Chay:
    python gold_qttg.py \
        --silver-dir output/spark_lake/silver \
        --output-dir output/spark_lake/gold
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

MASTER_SILVER_SUBDIR = "qttg_bhxh"
DETAIL_SILVER_SUBDIR = "qttg_bhxh_detail"
GOLD_SUBDIR = "bao_cao_bhxh_thang"

MONTH_REGEX = r"^[0-9]{4}(0[1-9]|1[0-2])$"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Gold bao cao thang QTTG BHXH")
    parser.add_argument(
        "--silver-dir",
        default="output/spark_lake/silver",
        help="Thu muc goc chua Silver (co qttg_bhxh, qttg_bhxh_detail)",
    )
    parser.add_argument(
        "--output-dir",
        default="output/spark_lake/gold",
        help="Thu muc goc de ghi Gold",
    )
    return parser.parse_args()


def build_dim_thang(spark: SparkSession, detail_df):
    """Tao danh sach thang YYYYMM tu TU_THANG nho nhat den DEN_THANG lon nhat."""
    valid_month_df = detail_df.filter(
        F.col("TU_THANG").rlike(MONTH_REGEX) & F.col("DEN_THANG").rlike(MONTH_REGEX)
    )

    bounds = valid_month_df.agg(
        F.min("TU_THANG").alias("MIN_THANG"),
        F.max("DEN_THANG").alias("MAX_THANG"),
    ).collect()[0]

    min_thang, max_thang = bounds["MIN_THANG"], bounds["MAX_THANG"]
    if min_thang is None or max_thang is None:
        return spark.createDataFrame([], "THANG_ID STRING"), None, None

    months = []
    y, m = int(min_thang[:4]), int(min_thang[4:6])
    y_end, m_end = int(max_thang[:4]), int(max_thang[4:6])
    while (y, m) <= (y_end, m_end):
        months.append((f"{y:04d}{m:02d}",))
        m += 1
        if m > 12:
            m = 1
            y += 1

    dim_thang_df = spark.createDataFrame(months, ["THANG_ID"])
    return dim_thang_df, min_thang, max_thang


def build_gold_report(dim_thang_df, master_df, detail_df):
    master_slim_df = master_df.select(
        F.col("ID").alias("_MASTER_ID"), F.col("SO_SO_BHXH")
    )

    # Chi lay detail co format thang hop le de tham gia bao cao (Silver da
    # tach rieng detail loi ra qttg_bhxh_detail_rejects nen ve ly thuyet
    # o day da sach, nhung loc lai mot lan nua cho chac truoc khi range-join).
    detail_valid_df = detail_df.filter(
        F.col("TU_THANG").rlike(MONTH_REGEX) & F.col("DEN_THANG").rlike(MONTH_REGEX)
    )

    detail_with_person_df = detail_valid_df.join(
        master_slim_df,
        detail_valid_df["MASTER_ID"] == master_slim_df["_MASTER_ID"],
        "inner",
    ).drop("_MASTER_ID")

    # Broadcast DIM_THANG (chi vai chuc/vai tram dong) de range-join voi
    # detail (co the len den hang trieu dong) khong bi shuffle nang.
    joined_df = detail_with_person_df.join(
        broadcast(dim_thang_df),
        (F.col("THANG_ID") >= F.col("TU_THANG")) & (F.col("THANG_ID") <= F.col("DEN_THANG")),
        "inner",
    )

    monthly_agg_df = joined_df.groupBy("THANG_ID").agg(
        F.countDistinct("SO_SO_BHXH").alias("SO_NGUOI_THAM_GIA"),
        F.countDistinct("MA_DON_VI").alias("SO_DON_VI"),
        F.sum("MUC_LUONG").alias("TONG_QUY_LUONG"),
        F.avg(F.when(F.col("MUC_LUONG") > 0, F.col("MUC_LUONG"))).alias("LUONG_BINH_QUAN"),
        F.countDistinct(
            F.when(F.col("MUC_LUONG") == 0, F.col("SO_SO_BHXH"))
        ).alias("SO_NGUOI_LUONG_0"),
    )

    # Left join lai voi DIM_THANG day du de khong bi "thieu thang" neu thang
    # do khong co detail nao khop (dien 0 cho cac chi tieu, giu THANG_ID).
    gold_df = (
        dim_thang_df.join(monthly_agg_df, "THANG_ID", "left")
        .fillna(
            {
                "SO_NGUOI_THAM_GIA": 0,
                "SO_DON_VI": 0,
                "TONG_QUY_LUONG": 0.0,
                "LUONG_BINH_QUAN": 0.0,
                "SO_NGUOI_LUONG_0": 0,
            }
        )
        .orderBy("THANG_ID")
    )
    return gold_df


def write_and_count(spark: SparkSession, df, output_path: str) -> int:
    df.write.mode("overwrite").parquet(output_path)
    written_df = spark.read.parquet(output_path)
    return written_df.count()


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("gold_qttg")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    silver_dir = args.silver_dir.rstrip("/")
    output_dir = args.output_dir.rstrip("/")

    master_path = f"{silver_dir}/{MASTER_SILVER_SUBDIR}"
    detail_path = f"{silver_dir}/{DETAIL_SILVER_SUBDIR}"
    gold_output = f"{output_dir}/{GOLD_SUBDIR}"

    print(f"[GOLD] Doc Silver master tu: {master_path}")
    master_df = spark.read.parquet(master_path)
    master_rows = master_df.count()

    print(f"[GOLD] Doc Silver detail tu: {detail_path}")
    detail_df = spark.read.parquet(detail_path)
    detail_rows = detail_df.count()

    print("[GOLD] Tao DIM_THANG tu khoang TU_THANG/DEN_THANG cua detail...")
    dim_thang_df, min_thang, max_thang = build_dim_thang(spark, detail_df)
    dim_thang_count = dim_thang_df.count()

    if dim_thang_count == 0:
        print("[GOLD] LOI: khong tao duoc DIM_THANG (detail khong co thang hop le).")
        spark.stop()
        sys.exit(1)

    print(f"[GOLD] DIM_THANG: {dim_thang_count} thang, tu {min_thang} den {max_thang}")

    print("[GOLD] Tinh bao cao theo thang...")
    gold_df = build_gold_report(dim_thang_df, master_df, detail_df)

    print(f"[GOLD] Ghi bao cao ra: {gold_output}")
    gold_written_rows = write_and_count(spark, gold_df, gold_output)

    # ---- Kiem tra sau khi chay ----
    errors = []

    if gold_written_rows != dim_thang_count:
        errors.append(
            f"So dong Gold ({gold_written_rows}) khac so thang trong DIM_THANG ({dim_thang_count})"
        )

    duplicate_months = (
        gold_df.groupBy("THANG_ID").count().filter(F.col("count") > 1).count()
    )
    if duplicate_months > 0:
        errors.append(f"Co {duplicate_months} THANG_ID bi trung dong trong bao cao Gold")

    negative_fund_months = gold_df.filter(F.col("TONG_QUY_LUONG") < 0).count()
    if negative_fund_months > 0:
        errors.append(f"Co {negative_fund_months} thang co TONG_QUY_LUONG am")

    if errors:
        for e in errors:
            print(f"[GOLD] LOI: {e}")
        print(f"LAYER=GOLD STATUS=FAILED MONTHS={gold_written_rows}")
        spark.stop()
        sys.exit(1)

    # ---- So lieu tong hop de kiem tra bang mat ----
    summary = gold_df.agg(
        F.max("SO_NGUOI_THAM_GIA").alias("MAX_NGUOI_THAM_GIA"),
        F.sum("TONG_QUY_LUONG").alias("SUM_QUY_LUONG_TAT_CA_THANG"),
        F.sum("SO_NGUOI_LUONG_0").alias("SUM_NGUOI_LUONG_0"),
    ).collect()[0]

    print(
        f"[GOLD] Input Silver: {master_rows} nguoi, {detail_rows} dong detail"
    )
    print(
        f"[GOLD] Bao cao: {gold_written_rows} thang (tu {min_thang} den {max_thang}), "
        f"thang dong nhat co {summary['MAX_NGUOI_THAM_GIA']} nguoi tham gia, "
        f"tong quy luong toan bo cac thang = {summary['SUM_QUY_LUONG_TAT_CA_THANG']:,.0f}, "
        f"tong luot nguoi luong 0 (cong don qua cac thang) = {summary['SUM_NGUOI_LUONG_0']}"
    )
    print(f"LAYER=GOLD STATUS=SUCCESS MONTHS={gold_written_rows}")
    spark.stop()


if __name__ == "__main__":
    main()
