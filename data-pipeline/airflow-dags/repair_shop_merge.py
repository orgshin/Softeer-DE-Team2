# dags/repair_shop_review_grade_dag.py
from datetime import timedelta
import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook
from slack_alarm import send_slack_alert_on_failure

# 설정 파일 (버킷/키/PG 테이블명/리소스 등)
CONFIG_FILE_PATH = "/opt/airflow/config/repair_shop/review_grade.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

S3_CONFIG = CONFIG["s3"]
POSTGRES_CONFIG = CONFIG["postgres"]
SPARK_CONFIG = CONFIG.get("spark", {})

# Airflow Connections
s3_conn: Connection = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
pg_conn: Connection = BaseHook.get_connection(POSTGRES_CONFIG["pg_conn_id"])

# Spark S3 conf (크레덴셜 주입)
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": s3_conn.login,
    "spark.hadoop.fs.s3a.secret.key": s3_conn.password,
}

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(hours=1),
    "on_failure_callback": send_slack_alert_on_failure,
}

with DAG(
    dag_id="repair_shop_review_grade_pipeline",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["repair_shop", "spark", "postgres", "etl", "grade"],
    default_args=default_args,
    doc_md="리뷰 Parquet → BERT분석 → 표준매핑 → 공업사등급 → PostgreSQL",
) as dag:
    SPARK_CONN_ID = CONFIG.get("spark_conn_id", "conn_spark")
    SPARK_JOB_FILE = CONFIG.get(
        "spark_job_file",
        "/opt/bitnami/spark/work/transformation/repair_shop_to_dw.py",
    )

    start = EmptyOperator(task_id="start")

    # application_args로 모두 전달 (환경변수 사용 X)
    app_args = [
        "--s3-bucket", S3_CONFIG["base_bucket"],
        "--reviews-prefix", S3_CONFIG.get("reviews_prefix", "repair_shop/*/*/*.parquet"),
        "--standards-key", S3_CONFIG["standards_key"],                 # 예: standards/전국자동차정비업체표준데이터.csv
        "--standards-encoding", S3_CONFIG.get("standards_encoding", "cp949"),
        "--pg-host", pg_conn.host,
        "--pg-port", str(pg_conn.port),
        "--pg-db", pg_conn.schema,
        "--pg-user", pg_conn.login,
        "--pg-password", pg_conn.password,
        "--pg-table", POSTGRES_CONFIG.get("table_name", "shop_review_with_grade"),
        "--pg-sslmode", POSTGRES_CONFIG.get("sslmode", "require"),
        "--pg-mode", POSTGRES_CONFIG.get("write_mode", "overwrite"),
    ]

    submit_review_grade_job = SparkSubmitOperator(
        task_id="submit_repair_shop_review_grade_job",
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOB_FILE,
        application_args=app_args,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4",
        conf=SPARK_S3_CONF,
        verbose=True,
    )

    end = EmptyOperator(task_id="end")
    start >> submit_review_grade_job >> end
