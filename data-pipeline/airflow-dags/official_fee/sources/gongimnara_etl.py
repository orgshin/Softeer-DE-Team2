# gongimnara_etl_dag.py

from __future__ import annotations
from datetime import timedelta

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# fetcher.py와 parser.py의 run_fetcher, run_parser 함수를 import
# 실제 환경에서는 이 스크립트들이 Airflow의 PYTHONPATH에 포함되어야 합니다.
# 예: dags/scripts/fetcher.py , dags/scripts/parser.py
from fetchers.official_fee.gongimnara_fetcher import run_fetcher 
from parsers.official_fee.gongimnara_parser import run_parser
from utils.slack_alarm import send_slack_alert_on_failure

default_args = {
    "owner": "airflow",
    "retries": 1,  # 실패 시 1번 재시도
    "retry_delay": timedelta(hours=1),  # 재시도 간격은 1시간
    "on_failure_callback": send_slack_alert_on_failure, # 실패 시 실행할 함수 지정
}

with DAG(
    dag_id="gongimnara_etl_dag",
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    schedule="0 9 * * 1",  # 매주 월요일 오전 9시에 실행
    catchup=False,
    tags=["web-scraping", "etl", "gongimnara"],
    default_args=default_args,
    doc_md="""
    ### 공임나라 표준 정비공임 ETL DAG

    1. **Fetch**: 공임나라 웹사이트에서 카테고리별 정비공임 HTML을 스크래핑하여 S3에 저장합니다.
    2. **Parse**: S3에 저장된 HTML 파일들을 읽어 파싱하고, 정형 데이터로 변환하여 Parquet 형식으로 S3에 저장합니다.
    
    **필수 Airflow Connection:**
    - `aws_default` (ID): AWS S3에 접근하기 위한 자격증명 (IAM Role 또는 Access Key)
    """,
) as dag:

    fetch_to_s3 = PythonOperator(
        task_id="fetch_html_and_upload_to_s3",
        python_callable=run_fetcher,
    )

    parse_from_s3 = PythonOperator(
        task_id="parse_html_and_save_as_parquet",
        python_callable=run_parser,
    )

    fetch_to_s3 >> parse_from_s3