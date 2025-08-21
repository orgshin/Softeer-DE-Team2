import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook

# --- DAG 파일 상단에서 설정 파일을 한번만 읽습니다 ---
CONFIG_FILE_PATH = "/opt/airflow/config/parts/hyunki.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# --- 공통 설정 변수 구성 ---
S3_CONFIG = CONFIG["s3"]
POSTGRES_CONFIG = CONFIG["postgres"]

# --- Spark Job #1 (Merge)에 전달할 인자들을 구성합니다 ---
S3_URL_FOR_MERGE = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['dest_prefix']}/"

# --- Spark가 S3에 접근하기 위한 공통 설정 ---
s3_conn: Connection = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": s3_conn.login,
    "spark.hadoop.fs.s3a.secret.key": s3_conn.password,
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

# --- PostgreSQL 접속 정보를 구성합니다 ---
pg_conn: Connection = BaseHook.get_connection(POSTGRES_CONFIG["pg_conn_id"])

with DAG(
    dag_id="hyunki_merge_and_load_to_postgres",
    start_date=pendulum.datetime(2025, 8, 20, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["hyunki", "spark", "postgres", "etl"],
) as dag:
    SPARK_CONN_ID = "conn_spark"
    SPARK_MERGER_JOB_FILE = "/opt/bitnami/spark/work/transformation/merge_shopping_parts.py"
    # ✨ 새로 추가된 Spark 집계 스크립트의 경로
    SPARK_AGGREGATOR_JOB_FILE = "/opt/bitnami/spark/work/transformation/parts_dw_to_dm.py"

    start = EmptyOperator(task_id="start")

    # Task 1: S3의 Parquet 파일들을 병합하여 'parts' 테이블에 적재
    submit_spark_merge_to_postgres_task = SparkSubmitOperator(
        task_id="submit_spark_merge_to_postgres_task",
        conn_id=SPARK_CONN_ID,
        application=SPARK_MERGER_JOB_FILE,
        application_args=[
            "--s3-url", S3_URL_FOR_MERGE,
            "--pg-host", pg_conn.host,
            "--pg-port", str(pg_conn.port),
            "--pg-db", pg_conn.schema,
            "--pg-table", POSTGRES_CONFIG["parts_table_name"], # config에서 parts 테이블 이름 사용
            "--pg-user", pg_conn.login,
            "--pg-password", pg_conn.password,
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3',
        conf=SPARK_S3_CONF,
        verbose=True,
    )

    # ✨ Task 2: 'parts' 테이블을 읽어 가격을 집계하고 'parts_avg_price' 테이블에 저장
    submit_spark_aggregate_job_task = SparkSubmitOperator(
        task_id="submit_spark_aggregate_job_task",
        conn_id=SPARK_CONN_ID,
        application=SPARK_AGGREGATOR_JOB_FILE,
        application_args=[
            "--pg-host", pg_conn.host,
            "--pg-port", str(pg_conn.port),
            "--pg-db", pg_conn.schema,
            "--pg-user", pg_conn.login,
            "--pg-password", pg_conn.password,
            "--read-table", POSTGRES_CONFIG["parts_table_name"], # 이전 단계의 결과 테이블을 읽음
            "--write-table", POSTGRES_CONFIG["avg_price_table_name"], # 최종 집계 결과를 저장할 테이블
        ],
        # 이 작업은 S3에 접근하지 않으므로 postgresql 드라이버만 필요합니다.
        packages='org.postgresql:postgresql:42.7.3',
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    # ✨ 파이프라인 순서: 병합/적재 작업 >> 집계 작업
    start >> submit_spark_merge_to_postgres_task >> submit_spark_aggregate_job_task >> end
