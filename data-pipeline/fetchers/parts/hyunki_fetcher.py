# ==============================================================================
# etl_pipelines/parts/fetchers/hyunki_fetcher.py
#
# 현키몰 부품 정보 수집 (Fetcher) 스크립트
# - requests 라이브러리를 사용하여 정적 웹 페이지의 HTML을 수집합니다.
# - 카테고리 및 페이지 번호를 순회하며 각 페이지의 HTML을 S3에 저장합니다.
# ==============================================================================
import logging
import random
import time
import yaml
from typing import Generator, Tuple
from urllib.parse import urlencode
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ==============================================================================
# 웹 스크래핑 로직
# ==============================================================================

def fetch_page(url: str, cfg: dict, logger: logging.Logger) -> str | None:
    """
    주어진 URL의 HTML 내용을 가져옵니다.
    - 실패 시 설정된 횟수만큼 재시도합니다.
    - User-Agent를 매번 랜덤으로 변경합니다.
    """
    req_cfg = cfg["requests"]
    timeout = req_cfg["timeout"]
    retries = req_cfg["retries"]
    delay = req_cfg["backoff_base_sec"]
    headers = (req_cfg.get("headers") or {}).copy()
    user_agents = req_cfg.get("user_agents") or []

    for attempt in range(1, retries + 1):
        headers["User-Agent"] = random.choice(user_agents) if user_agents else "Mozilla/5.0"
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status() # 2xx 상태 코드가 아닐 경우 예외 발생
            # 인코딩이 잘못된 경우를 대비하여 apparent_encoding 사용
            if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "ascii"):
                resp.encoding = resp.apparent_encoding
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"[재시도 {attempt}/{retries}] 페이지 수집 실패: {url}. 오류: {e}")
            if attempt == retries:
                logger.error(f"[수집 실패] {retries}번 시도 후에도 {url} 페이지를 가져올 수 없습니다.")
                return None
            time.sleep(delay * attempt) # 재시도 간격 증가
    return None

def build_s3_key(prefix: str, category_name: str, page: int) -> str:
    """S3에 저장할 파일 경로(Key)를 생성합니다."""
    safe_category = category_name.replace("/", "_") # 경로 문자를 언더스코어로 변경
    return f"{prefix}/{safe_category}/page_{page}.html"

def iter_pages(cfg: dict, logger: logging.Logger) -> Generator[Tuple[str, str, dict], None, None]:
    """
    설정 파일을 기반으로 스크래핑할 페이지를 순회하며
    (S3 key, HTML, 메타데이터)를 생성(yield)하는 제너레이터입니다.
    """
    fetcher_cfg = cfg["fetcher"]
    site_cfg = fetcher_cfg["site"]
    delay_cfg = fetcher_cfg["delays"]
    s3_prefix = cfg["s3"]["source_prefix"]

    logger.info("=== HTML 다운로더 제너레이터 시작 ===")

    for endpoint in site_cfg["endpoints"]:
        category_name = endpoint.strip("/").split("/")[0]
        logger.info(f"--- 카테고리 처리 중: {category_name} ({endpoint}) ---")

        for page in range(site_cfg["start_page"], site_cfg["max_pages"] + 1):
            params = {site_cfg['page_param']: page}
            url = f"{site_cfg['base']}{endpoint}?{urlencode(params)}"
            
            html = fetch_page(url, cfg, logger)
            if not html:
                logger.warning(f"페이지 {page} 수집 실패로 '{category_name}' 카테고리를 중단합니다.")
                break

            key = build_s3_key(s3_prefix, category_name, page)
            meta = {"source_url": url, "category": category_name, "page": page}

            yield key, html, meta

            # 페이지 간 랜덤 딜레이
            time.sleep(random.uniform(delay_cfg["per_page_min_sec"], delay_cfg["per_page_max_sec"]))

        # 카테고리 간 딜레이
        if site_cfg["pause_between_categories_sec"] > 0:
            time.sleep(site_cfg["pause_between_categories_sec"])

    logger.info("=== HTML 다운로더 제너레이터 완료 ===")

# ==============================================================================
# Airflow PythonOperator에서 호출될 메인 실행 함수
# ==============================================================================

def run_downloader(config: dict, **context):
    """
    Airflow에서 호출하는 엔트리포인트 함수.
    설정 파일을 기반으로 웹 페이지를 다운로드하여 S3에 저장합니다.
    """

    cfg = config
    logger = context["ti"].log # Airflow Task Instance 로거 사용
    s3_cfg = cfg["s3"]
    
    hook = S3Hook(aws_conn_id=s3_cfg["aws_conn_id"])
    bucket = s3_cfg["source_bucket"]
    
    logger.info(f"S3 다운로드 시작 (Bucket: {bucket})")

    # iter_pages 제너레이터를 통해 HTML을 하나씩 받아와 S3에 업로드
    for key, html, meta in iter_pages(cfg, logger):
        hook.load_string(
            string_data=html,
            key=key,
            bucket_name=bucket,
            replace=True,
            encoding="utf-8",
        )
        logger.info(f"[S3 저장 완료] s3://{bucket}/{key} (메타데이터: {meta})")
        
    logger.info("S3 다운로드 프로세스를 완료했습니다.")