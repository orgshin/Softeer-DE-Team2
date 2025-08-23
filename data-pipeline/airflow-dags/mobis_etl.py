# /opt/airflow/dags/mobis_dag.py
import json
import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # ✨ Trigger Operator 임포트

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
}

# --- DAG 정의 ---
with DAG(
    dag_id="mobis_fetch_and_spark_parse_pipeline_v3",
    start_date=pendulum.datetime(2025, 8, 22, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["mobis", "spark", "s3", "v3-pattern"],
) as dag:
    SPARK_CONN_ID = "conn_spark"
    SPARK_JOB_FILE_PATH = "/opt/bitnami/spark/work/parsers/parts/mobis_parser.py"

    start = EmptyOperator(task_id="start")

    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_fetcher,
        op_kwargs={"config_path": CONFIG_FILE_PATH},
    )

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
    
    # ✨ 이 작업이 성공하면 'mobis_parquet_to_postgres_etl' DAG를 실행시킵니다.
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_to_postgres_dag",
        trigger_dag_id="mobis_parquet_to_postgres_etl",  # 실행시킬 DAG의 ID
        wait_for_completion=False, # 다음 DAG가 끝날 때까지 기다리지 않음
    )

    end = EmptyOperator(task_id="end")

    # ✨ 워크플로우에 트리거 태스크 추가
    start >> fetch_html_task >> submit_spark_job_task >> trigger_load_dag >> end