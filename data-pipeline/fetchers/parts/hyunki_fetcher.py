import logging
import random
import re
import time
from typing import Dict, Generator, Tuple
from urllib.parse import urlencode

import requests
import yaml
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ==============================================================================
# 웹 스크래핑 로직
# ==============================================================================

def fetch_page(url: str, cfg: dict, logger: logging.Logger) -> str | None:
    """지정된 URL의 HTML 내용을 가져옵니다."""
    req_cfg = cfg["fetcher"]["requests"]
    timeout = req_cfg["timeout"]
    retries = req_cfg["retries"]
    delay = req_cfg["backoff_base_sec"]
    headers = (req_cfg.get("headers") or {}).copy()
    user_agents = req_cfg.get("user_agents") or []

    for attempt in range(1, retries + 1):
        headers["User-Agent"] = random.choice(user_agents) if user_agents else "Mozilla/5.0"
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "ascii"):
                resp.encoding = resp.apparent_encoding
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"[RETRY {attempt}/{retries}] Failed to fetch {url}. Error: {e}")
            if attempt == retries:
                logger.error(f"[FAILED] Could not fetch {url} after {retries} attempts.")
                return None
            time.sleep(delay * attempt)
    return None

def build_s3_key(prefix: str, category_name: str, page: int) -> str:
    """S3에 저장할 파일 경로(Key)를 생성합니다."""
    safe_category = category_name.replace("/", "_")
    return f"{prefix}/{safe_category}/page_{page}.html"

def iter_pages(cfg: dict, logger: logging.Logger) -> Generator[Tuple[str, str, dict], None, None]:
    """설정 파일을 기반으로 스크래핑할 페이지를 순회하며 (S3 key, HTML, 메타데이터)를 반환합니다."""
    fetcher_cfg = cfg["fetcher"]
    site_cfg = fetcher_cfg["site"]
    delay_cfg = fetcher_cfg["delays"]
    s3_prefix = cfg["s3"]["source_prefix"]

    logger.info("=== HTML Downloader Generator Started ===")

    for endpoint in site_cfg["endpoints"]:
        category_name = endpoint.strip("/").split("/")[0]
        logger.info(f"--- Processing category: {category_name} ({endpoint}) ---")

        for page in range(site_cfg["start_page"], site_cfg["max_pages"] + 1):
            params = {site_cfg['page_param']: page}
            url = f"{site_cfg['base']}{endpoint}?{urlencode(params)}"
            
            html = fetch_page(url, cfg, logger)
            if not html:
                logger.warning(f"Stopping category '{category_name}' due to fetch failure at page {page}.")
                break

            key = build_s3_key(s3_prefix, category_name, page)
            meta = {"source_url": url, "category": category_name, "page": page}

            yield key, html, meta

            time.sleep(random.uniform(delay_cfg["per_page_min_sec"], delay_cfg["per_page_max_sec"]))

        if site_cfg["pause_between_categories_sec"] > 0:
            time.sleep(site_cfg["pause_between_categories_sec"])

    logger.info("=== HTML Downloader Generator Finished ===")

# ==============================================================================
# Airflow PythonOperator에서 호출될 메인 함수
# ==============================================================================

def run_downloader(config_path: str, **context):
    """
    설정 파일을 기반으로 웹 페이지를 다운로드하여 S3에 저장합니다.
    """
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    
    logger = context["ti"].log
    s3_cfg = cfg["s3"]
    
    hook = S3Hook(aws_conn_id=s3_cfg["aws_conn_id"])
    bucket = s3_cfg["source_bucket"]
    
    logger.info(f"Starting S3 download to bucket: {bucket}")

    for key, html, meta in iter_pages(cfg, logger):
        # S3에 파일이 이미 존재하는 경우 건너뛰기 (옵션)
        # if hook.check_for_key(key=key, bucket_name=bucket):
        #     logger.info(f"[S3 SKIP] Already exists: s3://{bucket}/{key}")
        #     continue

        hook.load_string(
            string_data=html,
            key=key,
            bucket_name=bucket,
            replace=True,
            encoding="utf-8",
        )
        logger.info(f"[S3 SAVED] s3://{bucket}/{key} (meta: {meta})")
        
    logger.info("S3 download process finished.")
