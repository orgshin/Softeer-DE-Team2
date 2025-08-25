import json
from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- Custom utils and functions ---
from fetchers.parts.hyunki_fetcher import run_downloader
from utils.slack_alarm import send_slack_alert_on_failure
from utils.dag_utils import load_hierarchical_config, get_spark_s3_conf

# 1. 상수 정의
SPECIFIC_CONFIG_PATH = "/opt/airflow/config/parts/sources/hyunki.yaml"
SPARK_JOB_FILE_PATH = "/opt/bitnami/spark/work/parsers/parts/hyunki_parser.py"
SPARK_CONN_ID = "conn_spark"

# 2. 설정 로딩
CONFIG = load_hierarchical_config(SPECIFIC_CONFIG_PATH)

S3_CONFIG = CONFIG["s3"]
PARSER_CONFIG = CONFIG["parser"]

# 3. 변수 준비
SPARK_S3_CONF = get_spark_s3_conf(S3_CONFIG["aws_conn_id"])
SOURCE_PATH = f"s3a://{S3_CONFIG['source_bucket']}/{S3_CONFIG['source_prefix']}/*/*.html"
DEST_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['dest_prefix']}/"
PARSER_CONFIG_JSON = json.dumps(PARSER_CONFIG)

default_args = {
    "owner": "airflow", "retries": 1, "retry_delay": timedelta(hours=1),
    "on_failure_callback": send_slack_alert_on_failure,
}

# 4. DAG 정의
with DAG(
    dag_id="parts_source_hyunki_etl_pipeline",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule=None, catchup=False, tags=["parts", "source", "hyunki"], default_args=default_args,
    doc_md="Hyunki 부품 정보를 수집하고 Spark로 파싱하여 S3에 Parquet으로 저장합니다."
) as dag:
    # 5. 태스크 정의
    start = EmptyOperator(task_id="start")
    
    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_downloader,
        op_kwargs={"config": CONFIG},
    )

    parse_html_task = SparkSubmitOperator(
        task_id="parse_html_with_spark",
        conn_id=SPARK_CONN_ID, application=SPARK_JOB_FILE_PATH,
        application_args=["--source-path", SOURCE_PATH, "--dest-path", DEST_PATH, "--parser-config-json", PARSER_CONFIG_JSON],
        packages='org.apache.hadoop:hadoop-aws:3.3.4', conf=SPARK_S3_CONF, verbose=True,
    )

    trigger_merge_dag = TriggerDagRunOperator(
        task_id="trigger_merge_dag",
        trigger_dag_id="parts_pipeline_merge_all", # 후속 파이프라인 DAG ID
        wait_for_completion=False,
    )

    end = EmptyOperator(task_id="end")

    # 6. 워크플로우 명시
    start >> fetch_html_task >> parse_html_task >> trigger_merge_dag >> end