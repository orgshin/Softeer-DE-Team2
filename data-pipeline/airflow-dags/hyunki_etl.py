import json
import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook

# 실제 fetcher 스크립트에서 run_downloader 함수를 import 합니다.
from parts.hyunki_fetcher import run_downloader

# --- DAG 파일 상단에서 설정 파일을 한번만 읽습니다 ---
CONFIG_FILE_PATH = "/opt/airflow/config/parts/hyunki.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# --- Spark Job에 전달할 인자들을 구성합니다 ---
S3_CONFIG = CONFIG["s3"]
PARSER_CONFIG = CONFIG["parser"]

# 1. S3 경로 구성
SOURCE_PATH = f"s3a://{S3_CONFIG['source_bucket']}/{S3_CONFIG['source_prefix']}/*/*.html"
DEST_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['dest_prefix']}/"

# 2. 파싱 규칙을 JSON 문자열로 직렬화
PARSER_CONFIG_JSON = json.dumps(PARSER_CONFIG)

# 3. Spark가 S3에 접근하기 위한 AWS 인증 정보 및 Hadoop 설정
conn: Connection = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": conn.login,
    "spark.hadoop.fs.s3a.secret.key": conn.password,
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

with DAG(
    dag_id="hyunki_fetch_and_spark_parse_pipeline",
    # 2025년 8월 29일부터 매주 금요일 0시에 실행
    start_date=pendulum.datetime(2025, 8, 29, tz="Asia/Seoul"),
    schedule="0 0 * * FRI", # 1주일에 한번 (매주 금요일 0시)
    catchup=False,
    tags=["hyunki", "spark", "s3", "parser"],
) as dag:
    SPARK_CONN_ID = "conn_spark"
    SPARK_JOB_FILE_PATH = "/opt/bitnami/spark/work/parsers/parts/hyunki_parser.py"

    start = EmptyOperator(task_id="start")

    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_downloader,
        op_kwargs={"config_path": CONFIG_FILE_PATH},
    )

    submit_spark_parser_job = SparkSubmitOperator(
        task_id="submit_spark_parser_job",
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOB_FILE_PATH,
        application_args=[
            "--source-path", SOURCE_PATH,
            "--dest-path", DEST_PATH,
            "--parser-config-json", PARSER_CONFIG_JSON,
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4',
        conf=SPARK_S3_CONF,
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_html_task >> submit_spark_parser_job >> end