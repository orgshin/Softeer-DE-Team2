# ==============================================================================
# dags/utils/dag_utils.py
#
# Airflow DAG에서 공통으로 사용하는 유틸리티 함수 모음
# ==============================================================================
import yaml
import os
import glob
from collections.abc import Mapping
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection

# --- 상수 정의 ---
# Airflow 컨테이너 내부의 절대 경로를 사용합니다.
COMMON_CONFIG_DIR = "/opt/airflow/config/common"

def deep_merge(source: dict, destination: dict) -> dict:
    """두 딕셔너리를 재귀적으로 깊은 병합(deep merge)합니다."""
    for key, value in source.items():
        if isinstance(value, Mapping) and value:
            destination[key] = deep_merge(value, destination.get(key, {}))
        else:
            destination[key] = value
    return destination

def load_hierarchical_config(specific_config_path: str) -> dict:
    """
    1. /config/common/ 디렉토리의 모든 .yaml 파일을 자동으로 읽어 기본 설정을 만듭니다.
    2. specific_config_path의 특정 설정을 그 위에 덮어씁니다.
    """
    merged_config = {}
    
    # 1. 공통 설정 자동 로드 및 병합
    common_yaml_files = glob.glob(os.path.join(COMMON_CONFIG_DIR, "*.yaml"))
    common_yaml_files.extend(glob.glob(os.path.join(COMMON_CONFIG_DIR, "*.yml")))
    
    for path in sorted(common_yaml_files): # 파일 이름 순으로 일관되게 로드
        with open(path, "r", encoding="utf-8") as f:
            common_config = yaml.safe_load(f)
            if common_config:
                merged_config = deep_merge(common_config, merged_config)

    # 2. 특정 설정 로드 및 덮어쓰기
    with open(specific_config_path, "r", encoding="utf-8") as f:
        specific_config = yaml.safe_load(f)
        if specific_config:
            merged_config = deep_merge(specific_config, merged_config)
            
    return merged_config

def get_spark_s3_conf(aws_conn_id: str) -> dict:
    """Airflow Connection으로부터 Spark가 S3에 접근하기 위한 인증 정보를 생성합니다."""
    conn: Connection = BaseHook.get_connection(aws_conn_id)
    return {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key": conn.login,
        "spark.hadoop.fs.s3a.secret.key": conn.password,
    }

def get_pg_connection(pg_conn_id: str) -> Connection:
    """Airflow Connection으로부터 PostgreSQL 접속 정보를 가져옵니다."""
    return BaseHook.get_connection(pg_conn_id)