import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook

# --- Mobis 전용 설정 파일을 읽어옵니다 ---
CONFIG_FILE_PATH = "/opt/airflow/config/parts/mobis.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# --- 설정 변수 구성 ---
S3_CONFIG = CONFIG["s3"]
POSTGRES_CONFIG = CONFIG["postgres"]

# Spark Job에 전달할 Mobis Parquet 파일의 전체 경로를 구성합니다.
S3_FILE_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['prefix']}/mobis.parquet"

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
    dag_id="mobis_parquet_to_postgres_etl",
    start_date=pendulum.datetime(2025, 8, 21, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["mobis", "spark", "postgres", "etl"],
    doc_md="S3의 mobis.parquet 파일을 읽어 PostgreSQL에 적재하는 전용 파이프라인입니다."
) as dag:
    SPARK_CONN_ID = "conn_spark"
    # ✨ 실행할 Spark 스크립트 파일을 지정합니다.
    SPARK_MOBIS_JOB_FILE = "/opt/bitnami/spark/work/transformation/load_mobis_to_postgres.py"

    start = EmptyOperator(task_id="start")

    # Task 1: Mobis Parquet 파일을 읽어 PostgreSQL에 적재
    submit_spark_mobis_task = SparkSubmitOperator(
        task_id="submit_spark_load_mobis_task",
        conn_id=SPARK_CONN_ID,
        application=SPARK_MOBIS_JOB_FILE,
        application_args=[
            "--s3-path", S3_FILE_PATH,
            "--pg-host", pg_conn.host,
            "--pg-port", str(pg_conn.port),
            "--pg-db", pg_conn.schema,
            "--pg-table", POSTGRES_CONFIG["table_name"],
            "--pg-user", pg_conn.login,
            "--pg-password", pg_conn.password,  
            "--pg-sslmode", "require",      
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3',
        conf=SPARK_S3_CONF,
        verbose=True,
    )
    
    end = EmptyOperator(task_id="end")

    start >> submit_spark_mobis_task >> end
