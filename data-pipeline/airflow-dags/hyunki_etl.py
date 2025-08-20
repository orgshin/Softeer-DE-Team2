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

# 1. S3 경로 구성 (템플릿이 적용될 수 있도록 수정)
SOURCE_PATH = f"s3a://{S3_CONFIG['source_bucket']}/{S3_CONFIG['source_prefix']}/*/*.html"
# ✨ 수정: f-string 대신 문자열 접합을 사용하여 Jinja 템플릿을 보존합니다.
DEST_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['dest_prefix']}/"

# 2. 파싱 규칙을 JSON 문자열로 직렬화
PARSER_CONFIG_JSON = json.dumps(PARSER_CONFIG)

# 3. Spark가 S3에 접근하기 위한 AWS 인증 정보 및 Hadoop 설정
conn: Connection = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": conn.login,
    "spark.hadoop.fs.s3a.secret.key": conn.password,
}

print(SPARK_S3_CONF)

with DAG(
    dag_id="hyunki_fetch_and_spark_parse_pipeline",
    start_date=pendulum.datetime(2025, 8, 20, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["hyunki", "spark", "s3", "etl"],
) as dag:
    SPARK_CONN_ID = "conn_spark"
    SPARK_JOB_FILE_PATH = "/opt/bitnami/spark/work/parsers/parts/hyunki_parser.py"

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
            "--dest-path", DEST_PATH, # 이제 이 부분은 Airflow에 의해 올바르게 렌더링됩니다.
            "--parser-config-json", PARSER_CONFIG_JSON,
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf=SPARK_S3_CONF,
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_html_task >> submit_spark_job_task >> end
