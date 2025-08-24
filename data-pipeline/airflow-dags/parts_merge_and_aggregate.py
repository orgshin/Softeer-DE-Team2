import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook
from airflow.sensors.external_task import ExternalTaskSensor
import os

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
    "spark.driver.host": os.getenv("SPARK_DRIVER_HOST", "airflow-worker"),
    "spark.driver.bindAddress": os.getenv("SPARK_DRIVER_BIND_ADDRESS", "0.0.0.0"),
    "spark.driver.port": os.getenv("SPARK_DRIVER_PORT", "40000"),
    "spark.driver.blockManager.port": os.getenv("SPARK_DRIVER_BLOCK_MANAGER_PORT", "40001"),
}

# PostgreSQL 접속 정보 구성
pg_conn: Connection = BaseHook.get_connection(POSTGRES_CONFIG["pg_conn_id"])

with DAG(
    dag_id="parts_etl_merge_and_aggregate_all",
    # 선행 DAG들과 동일한 스케줄을 설정하여 실행일을 맞춥니다.
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule="0 0 * * FRI",
    catchup=False,
    tags=["parts", "spark", "postgres", "etl", "unified"],
    doc_md="S3 'parts' 경로 하위의 모든 소스 데이터를 병합하고, 그 결과를 집계하는 통합 ETL 파이프라인입니다."
) as dag:
    SPARK_CONN_ID = "conn_spark"
    SPARK_MERGER_JOB_FILE = "/opt/bitnami/spark/work/transformation/merge_shopping_parts.py"
    SPARK_AGGREGATOR_JOB_FILE = "/opt/bitnami/spark/work/transformation/parts_dw_to_dm.py"

    start = EmptyOperator(task_id="start")

    # Sensor 1: hyunki_fetch_and_spark_parse_pipeline DAG의 'end' 작업이 성공했는지 확인
    wait_for_hyunki_dag = ExternalTaskSensor(
        task_id="wait_for_hyunki_dag",
        external_dag_id="hyunki_fetch_and_spark_parse_pipeline",
        external_task_id="end", # 해당 DAG의 마지막 task id
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60, # 60초마다 확인
        timeout=600, # 10분 동안 기다림
    )

    # Sensor 2: partsro_fetch_and_spark_parse_pipeline DAG의 'end' 작업이 성공했는지 확인
    wait_for_partsro_dag = ExternalTaskSensor(
        task_id="wait_for_partsro_dag",
        external_dag_id="partsro_fetch_and_spark_parse_pipeline",
        external_task_id="end", # 해당 DAG의 마지막 task id
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='poke',
        poke_interval=60,
        timeout=600,
    )

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

    # 의존성 설정: 두 Sensor가 모두 성공해야 Spark Merge Task가 실행됩니다.
    start >> [wait_for_hyunki_dag, wait_for_partsro_dag] >> submit_spark_merge_task >> submit_spark_aggregate_task >> end