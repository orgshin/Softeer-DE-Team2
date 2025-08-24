import json
import pendulum
import yaml
from datetime import timedelta  # 👈 1. 시간 간격을 위해 timedelta import

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.connection import Connection
from airflow.hooks.base import BaseHook
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook # 👈 2. Slack Hook import

# 실제 fetcher 스크립트에서 run_downloader 함수를 import 합니다.
from parts.hyunki_fetcher import run_downloader
from slack_alarm import send_slack_alert_on_failure

# --- DAG 파일 상단에서 설정 파일을 한번만 읽습니다 ---
CONFIG_FILE_PATH = "/opt/airflow/config/parts/hyunki.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# --- Spark Job에 전달할 인자들을 구성합니다 ---
S3_CONFIG = CONFIG["s3"]
PARSER_CONFIG = CONFIG["parser"]
SOURCE_PATH = f"s3a://{S3_CONFIG['source_bucket']}/{S3_CONFIG['source_prefix']}/*/*.html"
DEST_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['dest_prefix']}/"
PARSER_CONFIG_JSON = json.dumps(PARSER_CONFIG)
conn: Connection = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": conn.login,
    "spark.hadoop.fs.s3a.secret.key": conn.password,
}

# ⚙️ 4. 모든 Task에 적용될 기본 인자(default_args) 설정
default_args = {
    "owner": "airflow",
    "retries": 1,  # 실패 시 1번 재시도
    "retry_delay": timedelta(hours=1),  # 재시도 간격은 1시간
    "on_failure_callback": send_slack_alert_on_failure, # 실패 시 실행할 함수 지정
}


with DAG(
    dag_id="hyunki_fetch_and_spark_parse_pipeline",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["hyunki", "spark", "s3", "parser"],
    default_args=default_args, # 👈 5. DAG에 default_args 적용
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

    trigger_merge_and_load_dag = TriggerDagRunOperator(
        task_id="trigger_merge_and_load_dag",
        trigger_dag_id="hyunki_merge_and_load_to_postgres",
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    start >> fetch_html_task >> submit_spark_parser_job >> trigger_merge_and_load_dag >> end