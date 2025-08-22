# /opt/airflow/dags/mobis_dag.py
import json
import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook

# Fetcher 함수 임포트
from parts.mobis_fetcher import run_fetcher

# --- YAML 설정 파일 로드 ---
CONFIG_FILE_PATH = "/opt/airflow/config/parts/mobis.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# --- Spark Job 인자 준비 ---
S3_CONFIG = CONFIG["s3"]
PARSER_CONFIG = CONFIG["parser"]

# Fetcher가 생성할 경로와 일치하는 와일드카드 소스 경로
SOURCE_PATH = f"s3a://{S3_CONFIG['source_bucket']}/parts/mobis_parts/*/*/*/*.html"
DEST_PATH = f"s3a://{S3_CONFIG['dest_bucket']}/parts/mobis_parts/"
PARSER_CONFIG_JSON = json.dumps(PARSER_CONFIG)

# --- Spark를 위한 AWS 자격 증명 동적 로드 ---
conn = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": conn.login,
    "spark.hadoop.fs.s3a.secret.key": conn.password,
    # S3 호환 스토리지(MinIO 등) 사용 시 아래 설정 필요
    "spark.hadoop.fs.s3a.endpoint": S3_CONFIG.get("s3_endpoint", "s3.amazonaws.com"),
    "spark.hadoop.fs.s3a.path.style.access": "true",
}

# --- DAG 정의 ---
with DAG(
    dag_id="mobis_fetch_and_spark_parse_pipeline_v3",
    start_date=pendulum.datetime(2025, 8, 22, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["mobis", "spark", "s3", "v3-pattern"],
) as dag:
    SPARK_CONN_ID = "conn_spark" # Airflow Spark Connection ID
    # Spark Worker에 배포된 파서 스크립트의 절대 경로
    SPARK_JOB_FILE_PATH = "/opt/bitnami/spark/work/parsers/parts/mobis_parser.py"

    start = EmptyOperator(task_id="start")

    # Fetcher 태스크: 설정 파일 경로만 전달
    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_fetcher,
        op_kwargs={"config_path": CONFIG_FILE_PATH},
    )

    # Parser 태스크: Spark 잡 제출
    submit_spark_job_task = SparkSubmitOperator(
        task_id="submit_spark_parser_job",
        conn_id=SPARK_CONN_ID,
        application=SPARK_JOB_FILE_PATH,
        application_args=[
            "--source-path", SOURCE_PATH,
            "--dest-path", DEST_PATH,
            "--parser-config-json", PARSER_CONFIG_JSON,
        ],
        packages='org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262',
        conf=SPARK_S3_CONF,
        verbose=True,
    )

    end = EmptyOperator(task_id="end")

    # 워크플로우 정의
    start >> fetch_html_task >> submit_spark_job_task >> end