import pendulum
import yaml
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# 방금 작성한 Python 스크립트에서 함수를 import 합니다.
# 이 파일이 Airflow의 PYTHONPATH에 포함된 디렉토리에 있어야 합니다. (e.g., dags/scripts/)
from transformation.merge_official_fee import merge_s3_parquet_to_postgres

# --- DAG 파일 상단에서 설정 파일을 한번만 읽습니다 ---
# ✨ 수정: 설정 파일 경로를 gongimnara.yaml로 변경
CONFIG_FILE_PATH = "/opt/airflow/config/official_fee/gongimnara.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

# --- 공통 설정 변수 구성 ---
# ✨ 수정: 새로운 YAML 구조에 맞게 설정 값을 가져옵니다.
AWS_CONFIG = CONFIG["aws"]
OFFICIAL_FEE_CONFIG = CONFIG["official_fee"]
# postgres 접속 정보는 YAML에 없으므로, Airflow Connection에서 직접 가져오도록 가정합니다.
# 또는 YAML 파일에 postgres 섹션을 추가할 수 있습니다.
POSTGRES_CONN_ID = "conn_postgres" # Airflow에 설정된 기본 Postgres Connection ID

with DAG(
    dag_id="merge_official_fee_to_postgres",
    start_date=pendulum.datetime(2025, 8, 21, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["official_fee", "s3", "postgres", "gongimnara"],
) as dag:
    start = EmptyOperator(task_id="start")

    merge_and_load_task = PythonOperator(
        task_id="merge_s3_parquet_and_load_to_postgres",
        python_callable=merge_s3_parquet_to_postgres,
        # op_kwargs를 통해 python_callable 함수에 파라미터를 전달합니다.
        # ✨ 수정: 새로운 변수명과 경로에서 값을 가져오도록 변경
        op_kwargs={
            "s3_conn_id": AWS_CONFIG["s3_connection_id"],
            "postgres_conn_id": POSTGRES_CONN_ID,
            "s3_bucket": OFFICIAL_FEE_CONFIG["bucket"],
            "s3_prefix": OFFICIAL_FEE_CONFIG["prefix"],
            "pg_table": OFFICIAL_FEE_CONFIG["table_name"],
        },
    )

    end = EmptyOperator(task_id="end")

    start >> merge_and_load_task >> end
