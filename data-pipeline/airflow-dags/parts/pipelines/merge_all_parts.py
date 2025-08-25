from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- Custom utils and functions ---
from utils.slack_alarm import send_slack_alert_on_failure
from utils.dag_utils import load_hierarchical_config, get_spark_s3_conf, get_pg_connection

# 1. 상수 정의
CONFIG_FILE_PATH = "/opt/airflow/config/parts/pipelines/merge_all_parts.yaml"
SPARK_MERGER_JOB_FILE = "/opt/bitnami/spark/work/transformation/merge_parts.py"
SPARK_AGGREGATOR_JOB_FILE = "/opt/bitnami/spark/work/transformation/aggregate_parts_price.py"
SPARK_CONN_ID = "conn_spark"

# 2. 설정 로딩
CONFIG = load_hierarchical_config(CONFIG_FILE_PATH)
S3_CONFIG = CONFIG["s3"]
POSTGRES_CONFIG = CONFIG["postgres"]

# 3. 변수 준비
SPARK_S3_CONF = get_spark_s3_conf(S3_CONFIG["aws_conn_id"])
pg_conn = get_pg_connection(POSTGRES_CONFIG["pg_conn_id"])
S3_BASE_URL = f"s3a://{S3_CONFIG['base_bucket']}/{S3_CONFIG['base_prefix']}"

default_args = {
    "owner": "airflow", "retries": 1, "retry_delay": timedelta(hours=1),
    "on_failure_callback": send_slack_alert_on_failure,
}

# 4. DAG 정의
with DAG(
    dag_id="parts_pipeline_merge_and_aggregate_all",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule="0 5 * * *", catchup=False, tags=["parts", "pipeline", "unified"], default_args=default_args,
    doc_md="S3 'parts' 경로의 모든 Parquet을 병합하고, 그 결과를 집계하는 통합 파이프라인입니다."
) as dag:
    # 5. 태스크 정의
    start = EmptyOperator(task_id="start")

    merge_all_sources_task = SparkSubmitOperator(
        task_id="merge_all_sources_to_dw",
        conn_id=SPARK_CONN_ID, application=SPARK_MERGER_JOB_FILE,
        application_args=[
            "--s3-url", S3_BASE_URL,
            "--pg-host", pg_conn.host, "--pg-port", str(pg_conn.port), "--pg-db", pg_conn.schema,
            "--pg-table", POSTGRES_CONFIG["parts_table_name"],
            "--pg-user", pg_conn.login, "--pg-password", pg_conn.password, "--pg-sslmode", "require",
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3',
        conf=SPARK_S3_CONF, verbose=True,
    )

    aggregate_price_task = SparkSubmitOperator(
        task_id="aggregate_price_to_dm",
        conn_id=SPARK_CONN_ID, application=SPARK_AGGREGATOR_JOB_FILE,
        application_args=[
            "--pg-host", pg_conn.host, "--pg-port", str(pg_conn.port), "--pg-db", pg_conn.schema,
            "--pg-user", pg_conn.login, "--pg-password", pg_conn.password, "--pg-sslmode", "require",
            "--read-table", POSTGRES_CONFIG["parts_table_name"],
            "--write-table", POSTGRES_CONFIG["avg_price_table_name"],
        ],
        packages='org.postgresql:postgresql:42.7.3', verbose=True,
    )

    end = EmptyOperator(task_id="end")

    # 6. 워크플로우 명시
    start >> merge_all_sources_task >> aggregate_price_task >> end