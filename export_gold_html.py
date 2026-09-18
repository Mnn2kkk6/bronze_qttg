"""
export_gold_html.py
=====================
Doc bao cao Gold (bao_cao_bhxh_thang) tu Parquet, dinh dang lai cho de nhin
(so co dau phan nghin, sap xep theo thang), xuat ra 1 file HTML de mo bang
trinh duyet.

Khong can Spark, chi can pandas + pyarrow (da cai san tu buoc xem Silver).

Chay:
    python export_gold_html.py --gold-dir output/spark_lake/gold --out bao_cao_bhxh_thang.html
"""

import argparse

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Xuat bao cao Gold ra HTML")
    parser.add_argument(
        "--gold-dir",
        default="output/spark_lake/gold",
        help="Thu muc goc chua Gold (co bao_cao_bhxh_thang)",
    )
    parser.add_argument(
        "--out",
        default="bao_cao_bhxh_thang.html",
        help="Ten file HTML se xuat ra",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    gold_path = f"{args.gold_dir}/bao_cao_bhxh_thang"

    df = pd.read_parquet(gold_path)
    df = df.sort_values("THANG_ID").reset_index(drop=True)

    # Dinh dang lai cho de nhin: THANG_ID -> YYYY-MM, so co dau phan nghin.
    display_df = pd.DataFrame(
        {
            "Tháng": df["THANG_ID"].str.slice(0, 4) + "-" + df["THANG_ID"].str.slice(4, 6),
            "Số người tham gia": df["SO_NGUOI_THAM_GIA"].map("{:,.0f}".format),
            "Số đơn vị": df["SO_DON_VI"].map("{:,.0f}".format),
            "Tổng quỹ lương (đ)": df["TONG_QUY_LUONG"].map("{:,.0f}".format),
            "Lương bình quân (đ)": df["LUONG_BINH_QUAN"].map("{:,.0f}".format),
            "Số người lương 0": df["SO_NGUOI_LUONG_0"].map("{:,.0f}".format),
        }
    )

    total_months = len(display_df)
    html_table = display_df.to_html(index=False, border=0, classes="report-table")

    html_page = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="utf-8">
<title>Bao cao tham gia BHXH theo thang</title>
<style>
  body {{ font-family: Segoe UI, Arial, sans-serif; margin: 24px; background: #f7f7f9; }}
  h1 {{ font-size: 20px; }}
  .meta {{ color: #555; margin-bottom: 16px; }}
  table.report-table {{
    border-collapse: collapse;
    width: 100%;
    background: #fff;
    box-shadow: 0 1px 3px rgba(0,0,0,0.1);
  }}
  table.report-table th, table.report-table td {{
    border: 1px solid #ddd;
    padding: 6px 10px;
    text-align: right;
    font-size: 13px;
    white-space: nowrap;
  }}
  table.report-table th:first-child, table.report-table td:first-child {{
    text-align: left;
  }}
  table.report-table th {{
    background: #2c3e50;
    color: #fff;
    position: sticky;
    top: 0;
  }}
  table.report-table tr:nth-child(even) {{ background: #f2f2f2; }}
</style>
</head>
<body>
<h1>Báo cáo tổng hợp tham gia BHXH theo tháng</h1>
<div class="meta">Nguồn: output/spark_lake/gold/bao_cao_bhxh_thang &middot; Tổng {total_months} tháng</div>
{html_table}
</body>
</html>
"""

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html_page)

    print(f"Da xuat {total_months} thang ra: {args.out}")


if __name__ == "__main__":
    main()