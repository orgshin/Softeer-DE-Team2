from datetime import timedelta
import os

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from parts.hyunki_fetcher import iter_pages, load_config

DAG_ID = "hyunki_html_downloader_s3hook"
DEFAULT_CONFIG_PATH = "/opt/airflow/config/parts/hyunki.yaml"

def _download_and_store(**context):
    config_path = Variable.get("HYUNKI_CONFIG_PATH", DEFAULT_CONFIG_PATH)
    cfg = load_config(config_path)

    storage_target = (cfg["general"].get("storage_target") or "s3").lower()

    if storage_target == "s3" and (cfg.get("s3", {}).get("enabled", False)):
        s3_cfg = cfg["s3"]
        aws_conn_id = s3_cfg.get("aws_conn_id", "aws_default")
        bucket = s3_cfg["bucket"]
        skip_if_exists = bool(s3_cfg.get("skip_if_exists", True))

        hook = S3Hook(aws_conn_id=aws_conn_id)

        for key, html, meta in iter_pages(config_path):
            # 존재하면 스킵
            if skip_if_exists and hook.check_for_key(key=key, bucket_name=bucket):
                # Airflow 로그에 남김
                print(f"[S3 SKIP] Already exists: s3://{bucket}/{key}")
                continue

            # 업로드 (S3Hook은 메타데이터/ACL 세부는 제한적. 필요 시 load_bytes + ExtraArgs 고려)
            hook.load_string(
                string_data=html,
                key=key,
                bucket_name=bucket,
                replace=True,       # skip_if_exists가 False면 무조건 덮어쓰기
                encrypt=False,      # 필요 시 True (버킷 암호화 정책 따르는 걸 권장)
                encoding="utf-8",
            )
            print(f"[S3 SAVED] s3://{bucket}/{key}  (meta: {meta})")

    else:
        # LOCAL 저장 옵션 (테스트용)
        out_dir = cfg["general"]["html_output_dir"]
        os.makedirs(out_dir, exist_ok=True)
        for key, html, meta in iter_pages(config_path):
            # key를 파일 경로로 사용
            file_path = os.path.join(out_dir, key)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # 존재 체크
            if os.path.exists(file_path) and cfg.get("s3", {}).get("skip_if_exists", True):
                print(f"[LOCAL SKIP] {file_path}")
                continue
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(html)
            print(f"[LOCAL SAVED] {file_path}  (meta: {meta})")


with DAG(
    dag_id=DAG_ID,
    description="Download Hyunki category pages and store to S3 using Airflow S3Hook.",
    schedule=None,  # 필요시 크론식 e.g. "0 3 * * *"
    start_date=pendulum.datetime(2025, 8, 1, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-pipeline",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["hyunki", "crawler", "s3", "hook"],
) as dag:

    download_html_to_s3 = PythonOperator(
        task_id="download_html_to_s3",
        python_callable=_download_and_store,
        provide_context=True,
    )

    download_html_to_s3
