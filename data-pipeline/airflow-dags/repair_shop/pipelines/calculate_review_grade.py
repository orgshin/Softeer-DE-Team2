from datetime import timedelta
import pendulum
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# --- Custom utils and functions ---
from utils.slack_alarm import send_slack_alert_on_failure
from utils.dag_utils import load_hierarchical_config, get_spark_s3_conf, get_pg_connection

# 1. 상수 정의
CONFIG_FILE_PATH = "/opt/airflow/config/repair_shop/pipelines/calculate_review_grade.yaml"
SPARK_JOB_FILE = "/opt/bitnami/spark/work/transformation/calculate_shop_grades.py"
SPARK_CONN_ID = "conn_spark"

# 2. 설정 로딩
CONFIG = load_hierarchical_config(CONFIG_FILE_PATH)
S3_CONFIG = CONFIG["s3"]
POSTGRES_CONFIG = CONFIG["postgres"]

# 3. 변수 준비
SPARK_S3_CONF = get_spark_s3_conf(S3_CONFIG["aws_conn_id"])
pg_conn = get_pg_connection(POSTGRES_CONFIG["pg_conn_id"])

app_args = [
    "--s3-bucket", S3_CONFIG["base_bucket"],
    "--reviews-prefix", S3_CONFIG.get("reviews_prefix", "repair_shop/*/*/*.parquet"),
    "--standards-key", S3_CONFIG["standards_key"],
    "--standards-encoding", S3_CONFIG.get("standards_encoding", "cp949"),
    "--pg-host", pg_conn.host, "--pg-port", str(pg_conn.port), "--pg-db", pg_conn.schema,
    "--pg-user", pg_conn.login, "--pg-password", pg_conn.password,
    "--pg-table", POSTGRES_CONFIG.get("table_name", "shop_review_with_grade"),
    "--pg-sslmode", POSTGRES_CONFIG.get("sslmode", "require"),
    "--pg-mode", POSTGRES_CONFIG.get("write_mode", "overwrite"),
]

default_args = {
    "owner": "airflow", "retries": 1, "retry_delay": timedelta(hours=1),
    "on_failure_callback": send_slack_alert_on_failure,
}

# 4. DAG 정의
with DAG(
    dag_id="repair_shop_job_calculate_review_grade",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule="0 6 * * *", catchup=False, tags=["repair_shop", "job", "grade"], default_args=default_args,
    doc_md="수집된 정비소 리뷰를 분석하여 등급을 계산하고 PostgreSQL에 저장합니다."
) as dag:
    # 5. 태스크 정의
    start = EmptyOperator(task_id="start")

    submit_grade_job = SparkSubmitOperator(
        task_id="submit_review_grade_spark_job",
        conn_id=SPARK_CONN_ID, application=SPARK_JOB_FILE, application_args=app_args,
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4",
        conf=SPARK_S3_CONF, verbose=True,
    )

    end = EmptyOperator(task_id="end")
    
    # 6. 워크플로우 명시
    start >> submit_grade_job >> end