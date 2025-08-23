# cardoc_etl_pipeline.py (형식 유지 / Spark 앱만 교체)

import pendulum, yaml
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.hooks.base import BaseHook

from repair_shop.cardoc_fetcher import run_fetcher

CONFIG_FILE_PATH = "/opt/airflow/config/repair_shop/cardoc.yaml"
with open(CONFIG_FILE_PATH, "r", encoding="utf-8") as f:
    CONFIG = yaml.safe_load(f)

S3_CONFIG = CONFIG["s3"]

conn = BaseHook.get_connection(S3_CONFIG["aws_conn_id"])
SPARK_S3_CONF = {
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.access.key": conn.login,
    "spark.hadoop.fs.s3a.secret.key": conn.password,
}

with DAG(
    dag_id="cardoc_etl_pipeline",
    start_date=pendulum.datetime(2025, 8, 23, tz="Asia/Seoul"),
    schedule="0 3 * * *",
    catchup=False,
    tags=["cardoc", "etl", "spark", "s3"],
) as dag:
    
    fetch_html_task = PythonOperator(
        task_id="fetch_html_to_s3",
        python_callable=run_fetcher,
        op_kwargs={
            "config_path": CONFIG_FILE_PATH,
            "ds_nodash": "{{ ds_nodash }}",
        },
    )

    parse_html_with_spark_task = SparkSubmitOperator(
        task_id="parse_html_with_spark",
        conn_id="conn_spark",
        application="/opt/bitnami/spark/work/parsers/repair_shop/cardoc_parser.py",
        application_args=[
            # fetcher가 XCom으로 반환한 "날짜 prefix"를 그대로 이어받아 사용
            "--input-path",
            f"s3a://{S3_CONFIG['source_bucket']}/{{{{ ti.xcom_pull(task_ids='fetch_html_to_s3') }}}}",
            "--output-path",
            f"s3a://{S3_CONFIG['dest_bucket']}/{S3_CONFIG['parser_output_prefix']}/{{{{ ds_nodash }}}}",
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.4",
        conf=SPARK_S3_CONF,
        verbose=True,
    )
    
    fetch_html_task >> parse_html_with_spark_task
