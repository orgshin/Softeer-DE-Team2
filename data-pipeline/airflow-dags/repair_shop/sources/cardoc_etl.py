from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- Custom utils and functions ---
from fetchers.repair_shop.cardoc_fetcher import run_fetcher
from utils.slack_alarm import send_slack_alert_on_failure
from utils.dag_utils import load_hierarchical_config, get_spark_s3_conf

# 1. 상수 정의
CONFIG_FILE_PATH = "/opt/airflow/config/repair_shop/sources/cardoc.yaml"
SPARK_JOB_FILE_PATH = "/opt/bitnami/spark/work/parsers/repair_shop/cardoc_parser.py"
SPARK_CONN_ID = "conn_spark"

# 2. 설정 로딩
CONFIG = load_hierarchical_config(CONFIG_FILE_PATH)
S3_CONFIG = CONFIG["s3"]

# 3. 변수 준비
SPARK_S3_CONF = get_spark_s3_conf(S3_CONFIG["aws_conn_id"])

default_args = {
    "owner": "airflow", "retries": 1, "retry_delay": timedelta(hours=1),
    "on_failure_callback": send_slack_alert_on_failure,
}

# 4. DAG 정의
with DAG(
    dag_id="repair_shop_source_cardoc_etl_pipeline",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule="0 3 * * *", catchup=False, tags=["repair_shop", "source", "cardoc"], default_args=default_args,
    doc_md="Cardoc 정비소 정보를 수집하고 Spark로 파싱하여 S3에 Parquet으로 저장합니다."
) as dag:
    # 5. 태스크 정의
    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_fetcher,
        op_kwargs={"config": CONFIG, "ds_nodash": "{{ ds_nodash }}"},
    )

    parse_html_task = SparkSubmitOperator(
        task_id="parse_html_with_spark",
        conn_id=SPARK_CONN_ID, application=SPARK_JOB_FILE_PATH,
        application_args=[
            "--input-path", f"s3a://{S3_CONFIG['source_bucket']}/{{{{ ti.xcom_pull(task_ids='fetch_html_to_s3') }}}}",
            "--output-path", f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['parser_output_prefix']}/{{{{ ds_nodash }}}}",
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4", conf=SPARK_S3_CONF, verbose=True,
    )

    # 6. 워크플로우 명시
    fetch_html_task >> parse_html_task