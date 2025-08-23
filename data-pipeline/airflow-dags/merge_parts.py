import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook

# --- 범용 설정 파일을 읽어옵니다 ---
CONFIG_FILE_PATH = "/opt/airflow/config/parts/merge.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# --- 설정 변수 구성 ---
S3_CONFIG = CONFIG["s3"]
POSTGRES_CONFIG = CONFIG["postgres"]

# Spark Job에 전달할 S3 기본 경로를 구성합니다.
S3_BASE_URL = f"s3a://{S3_CONFIG['base_bucket']}/{S3_CONFIG['base_prefix']}"

# Spark가 S3에 접근하기 위한 공통 설정
s3_conn: Connection = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": s3_conn.login,
    "spark.hadoop.fs.s3a.secret.key": s3_conn.password,
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

# PostgreSQL 접속 정보 구성
pg_conn: Connection = BaseHook.get_connection(POSTGRES_CONFIG["pg_conn_id"])

with DAG(
    dag_id="parts_etl_merge_and_aggregate_all",
    start_date=pendulum.datetime(2025, 8, 21, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["parts", "spark", "postgres", "etl", "unified"],
    doc_md="S3 'parts' 경로 하위의 모든 소스 데이터를 병합하고, 그 결과를 집계하는 통합 ETL 파이프라인입니다."
) as dag:
    SPARK_CONN_ID = "conn_spark"
    SPARK_MERGER_JOB_FILE = "/opt/bitnami/spark/work/transformation/merge_shopping_parts.py"
    SPARK_AGGREGATOR_JOB_FILE = "/opt/bitnami/spark/work/transformation/parts_dw_to_dm.py"

    start = EmptyOperator(task_id="start")

    # Task 1: S3의 모든 Parquet 파일들을 병합하여 'parts_merged' 테이블에 적재
    submit_spark_merge_task = SparkSubmitOperator(
        task_id="submit_spark_merge_all_sources_task",
        conn_id=SPARK_CONN_ID,
        application=SPARK_MERGER_JOB_FILE,
        application_args=[
            "--s3-url", S3_BASE_URL,
            "--pg-host", pg_conn.host,
            "--pg-port", str(pg_conn.port),
            "--pg-db", pg_conn.schema,
            "--pg-table", POSTGRES_CONFIG["parts_table_name"],
            "--pg-user", pg_conn.login,
            "--pg-password", pg_conn.password,
            "--pg-sslmode", "require",      
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3',
        conf=SPARK_S3_CONF,
        verbose=True,
    )

    # Task 2: 'parts_merged' 테이블을 읽어 가격을 집계하고 'parts_avg_price' 테이블에 저장
    submit_spark_aggregate_task = SparkSubmitOperator(
        task_id="submit_spark_aggregate_price_task",
        conn_id=SPARK_CONN_ID,
        application=SPARK_AGGREGATOR_JOB_FILE,
        application_args=[
            "--pg-host", pg_conn.host,
            "--pg-port", str(pg_conn.port),
            "--pg-db", pg_conn.schema,
            "--pg-user", pg_conn.login,
            "--pg-password", pg_conn.password,
            "--pg-sslmode", "require",      
            "--read-table", POSTGRES_CONFIG["parts_table_name"],
            "--write-table", POSTGRES_CONFIG["avg_price_table_name"],
        ],
        packages='org.postgresql:postgresql:42.7.3',
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> submit_spark_merge_task >> submit_spark_aggregate_task >> end
