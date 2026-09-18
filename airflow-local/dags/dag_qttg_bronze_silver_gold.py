"""
dag_qttg_bronze_silver_gold.py
================================
DAG dieu phoi pipeline QTTG BHXH: Bronze -> Silver -> Gold -> Validate.

Airflow khong chay logic ETL truc tiep - moi task chi goi `docker exec`
vao container Spark Master roi `spark-submit` mot file PySpark da co san
trong spark/apps/qttg, theo dung kien truc trong HUONG_DAN_AIRFLOW_SPARK_LOCAL.md:

    BashOperator -> Docker socket -> docker exec <spark-master> -> spark-submit

Doi ten SPARK_CONTAINER cho khop voi cot NAMES tra ve tu:
    docker ps --format "table {{.Names}}"
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Doi ten nay neu container Spark Master cua ban ten khac.
# Kiem tra bang: docker ps --format "table {{.Names}}"
SPARK_CONTAINER = "qttg-spark-master"
SPARK_MASTER = "spark://spark-master:7077"
APP_DIR = "/opt/spark/apps/qttg"
DATA_DIR = "/opt/spark/data"


def spark_submit(app_name: str, app_args: str = "") -> str:
    """Tao lenh BashOperator: docker exec vao Spark Master roi spark-submit.

    `set -e` de BashOperator nhan dung exit code cua spark-submit (khong
    bi che boi exit code cua docker exec neu co nhieu lenh noi tiep).
    """
    return f"""
set -e
docker exec {SPARK_CONTAINER} \
  /opt/spark/bin/spark-submit \
  --master {SPARK_MASTER} \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 2g \
  --executor-cores 2 \
  {APP_DIR}/{app_name} {app_args}
""".strip()


default_args = {
    "owner": "student",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="qttg_bronze_silver_gold",
    description="Pipeline QTTG BHXH: Bronze -> Silver -> Gold -> Validate (Spark local qua Docker Desktop)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spark", "qttg", "local"],
) as dag:

    bronze_qttg = BashOperator(
        task_id="bronze_qttg",
        bash_command=spark_submit(
            "bronze_qttg.py",
            f"--input-dir file://{DATA_DIR}/raw_qttg_1m "
            f"--output-dir file://{DATA_DIR}/lake/bronze",
        ),
        execution_timeout=timedelta(hours=1),
    )

    silver_qttg = BashOperator(
        task_id="silver_qttg",
        bash_command=spark_submit(
            "silver_qttg.py",
            f"--bronze-dir file://{DATA_DIR}/lake/bronze "
            f"--output-dir file://{DATA_DIR}/lake/silver",
        ),
        execution_timeout=timedelta(hours=1),
    )

    gold_qttg = BashOperator(
        task_id="gold_qttg",
        bash_command=spark_submit(
            "gold_qttg.py",
            f"--silver-dir file://{DATA_DIR}/lake/silver "
            f"--output-dir file://{DATA_DIR}/lake/gold",
        ),
        execution_timeout=timedelta(hours=1),
    )

    validate_qttg = BashOperator(
        task_id="validate_qttg",
        bash_command=spark_submit(
            "validate_qttg.py",
            f"--lake-dir file://{DATA_DIR}/lake",
        ),
        execution_timeout=timedelta(minutes=30),
    )

    bronze_qttg >> silver_qttg >> gold_qttg >> validate_qttg
