from datetime import timedelta
import json
import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook

# Fetcher 함수 임포트
from parts.partsro_fetcher import run_downloader
from slack_alarm import send_slack_alert_on_failure

# --- YAML 설정 파일 로드 --- 
CONFIG_FILE_PATH = "/opt/airflow/config/parts/partsro.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

default_args = {
    "owner": "airflow",
    "retries": 1,  # 실패 시 1번 재시도
    "retry_delay": timedelta(hours=1),  # 재시도 간격은 1시간
    "on_failure_callback": send_slack_alert_on_failure, # 실패 시 실행할 함수 지정
}

# --- Spark Job 인자 준비 ---
S3_CONFIG = CONFIG["s3"]
PARSER_CONFIG = CONFIG["parser"]

SOURCE_PATH = f"s3a://{S3_CONFIG['source_bucket']}/{S3_CONFIG['source_prefix']}/*/*.html"
DEST_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['dest_prefix']}/"
PARSER_CONFIG_JSON = json.dumps(PARSER_CONFIG)

# --- Spark를 위한 AWS 자격 증명 가져오기 ---
conn: Connection = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": conn.login,
    "spark.hadoop.fs.s3a.secret.key": conn.password,
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

# --- DAG 정의 ---
with DAG(
    dag_id="partsro_fetch_and_spark_parse_pipeline",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["partsro", "spark", "s3", "etl"],
    default_args=default_args,
) as dag:
    SPARK_CONN_ID = "conn_spark"
    # Spark Worker에 있는 파서 스크립트 경로
    SPARK_JOB_FILE_PATH = "/opt/bitnami/spark/work/parsers/parts/partsro_parser.py"

    start = EmptyOperator(task_id="start")

    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_downloader,
        op_kwargs={"config_path": CONFIG_FILE_PATH},
    )

    submit_spark_job_task = SparkSubmitOperator(
        task_id="submit_spark_parser_job",
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOB_FILE_PATH,
        application_args=[
            "--source-path", SOURCE_PATH,
            "--dest-path", DEST_PATH,
            "--parser-config-json", PARSER_CONFIG_JSON,
        ],
        # Spark가 S3와 통신하기 위해 필요한 패키지들
        packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf=SPARK_S3_CONF,
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_html_task >> submit_spark_job_task >> end