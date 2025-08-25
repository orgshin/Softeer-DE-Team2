import json
from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- Custom utils and functions ---
from fetchers.parts.mobis_fetcher import run_fetcher
from utils.slack_alarm import send_slack_alert_on_failure
from utils.dag_utils import load_hierarchical_config, get_spark_s3_conf, get_pg_connection

# 1. 상수 정의
CONFIG_FILE_PATH = "/opt/airflow/config/parts/sources/mobis.yaml"
SPARK_PARSER_FILE = "/opt/bitnami/spark/work/parsers/parts/mobis_parser.py"
SPARK_LOADER_FILE = "/opt/bitnami/spark/work/transformation/load_mobis.py"
SPARK_CONN_ID = "conn_spark"

# 2. 설정 로딩
CONFIG = load_hierarchical_config(CONFIG_FILE_PATH)
S3_CONFIG = CONFIG["s3"]
POSTGRES_CONFIG = CONFIG["postgres"]
PARSER_CONFIG = CONFIG["parser"]

# 3. 변수 준비
SPARK_S3_CONF = get_spark_s3_conf(S3_CONFIG["aws_conn_id"])
pg_conn = get_pg_connection(POSTGRES_CONFIG["pg_conn_id"])

SOURCE_PATH = f"s3a://{S3_CONFIG['source_bucket']}/{S3_CONFIG['prefix']}/*/*/*/*.html"
PARQUET_DEST_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['prefix']}/"
PARQUET_FILE_PATH = f"{PARQUET_DEST_PATH}*.parquet" # 로더가 사용할 최종 Parquet 파일 경로
PARSER_CONFIG_JSON = json.dumps(PARSER_CONFIG)

default_args = {
    "owner": "airflow", "retries": 1, "retry_delay": timedelta(hours=1),
    "on_failure_callback": send_slack_alert_on_failure,
}

# 4. DAG 정의
with DAG(
    dag_id="parts_source_mobis_etl_pipeline",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule=None, catchup=False, tags=["parts", "source", "mobis"], default_args=default_args,
    doc_md="Mobis 부품 정보를 수집, 파싱하여 Parquet으로 저장 후, PostgreSQL에 최종 적재합니다."
) as dag:
    # 5. 태스크 정의
    start = EmptyOperator(task_id="start")

    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_fetcher,
        op_kwargs={"config": CONFIG},
    )

    parse_html_task = SparkSubmitOperator(
        task_id="parse_html_with_spark",
        conn_id=SPARK_CONN_ID, application=SPARK_PARSER_FILE,
        application_args=["--source-path", SOURCE_PATH, "--dest-path", PARQUET_DEST_PATH, "--parser-config-json", PARSER_CONFIG_JSON],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf=SPARK_S3_CONF, verbose=True,
    )

    load_to_postgres_task = SparkSubmitOperator(
        task_id="load_parquet_to_postgres",
        conn_id=SPARK_CONN_ID, application=SPARK_LOADER_FILE,
        application_args=[
            "--s3-path", PARQUET_FILE_PATH,
            "--pg-host", pg_conn.host, "--pg-port", str(pg_conn.port), "--pg-db", pg_conn.schema,
            "--pg-table", POSTGRES_CONFIG["table_name"],
            "--pg-user", pg_conn.login, "--pg-password", pg_conn.password, "--pg-sslmode", "require",
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3',
        conf=SPARK_S3_CONF, verbose=True,
    )
    
    end = EmptyOperator(task_id="end")

    # 6. 워크플로우 명시
    start >> fetch_html_task >> parse_html_task >> load_to_postgres_task >> end