from datetime import timedelta
import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# 역할에 따라 분리된 모듈에서 필요한 함수들을 import
from parts.hyunki_fetcher import run_downloader
from parsers.parts.hyunki_parser import parse_s3_html_to_s3_parquet

# Airflow 변수나 기본 경로를 상수로 정의
CONFIG_FILE_PATH = "/opt/airflow/config/parts/hyunki.yaml"
DAG_ID = "hyunki_fetch_and_parse_pipeline"

with DAG(
    dag_id=DAG_ID,
    description="Download Hyunki pages to S3 and then parse them into a Parquet file.",
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 20, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-pipeline",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["hyunki", "pipeline", "s3", "parquet"],
) as dag:

    # 1. Fetcher Task: HTML 다운로드 및 S3 저장
    download_html_task = PythonOperator(
        task_id="download_html_to_s3",
        python_callable=run_downloader,
        op_kwargs={"config_path": CONFIG_FILE_PATH},
    )

    # 2. Parser Task: S3의 HTML을 Parquet으로 변환하여 S3에 저장
    parse_and_store_task = PythonOperator(
        task_id="parse_html_and_store_as_parquet",
        python_callable=parse_s3_html_to_s3_parquet,
        op_kwargs={"config_path": CONFIG_FILE_PATH},
    )

    # Task 실행 순서 정의
    download_html_task >> parse_and_store_task