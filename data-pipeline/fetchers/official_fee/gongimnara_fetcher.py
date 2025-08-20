# fetcher.py

import os
import re
import time
import random
import logging
import yaml
import requests
from typing import Dict, Any, List
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ------------------- Config & Logging -------------------
def load_cfg(path: str = "gongimnara.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("gongim_fetcher")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        sh = logging.StreamHandler()
        fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        fh.setFormatter(formatter)
        sh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

# ------------------- HTTP Helpers (강화) -------------------
def pick_ua(cfg):
    uas = cfg["requests"].get("user_agents") or []
    return random.choice(uas) if uas else "Mozilla/5.0"

def http_request(session, method, url, cfg, **kwargs):
    timeout = cfg["requests"]["timeout"]
    retries = cfg["requests"]["retries"]
    backoff = float(cfg["requests"]["backoff_base"])
    
    headers = cfg["requests"].get("headers", {}).copy()
    headers["User-Agent"] = pick_ua(cfg)
    
    for attempt in range(1, retries + 1):
        try:
            r = session.request(method, url, headers=headers, timeout=timeout, **kwargs)
            r.raise_for_status()
            if not r.encoding or r.encoding.lower() in ("iso-8859-1", "ascii"):
                r.encoding = r.apparent_encoding
            return r.text
        except requests.RequestException as e:
            if attempt == retries:
                logging.error(f"{method.upper()} failed {url} -> {e}")
                raise
            time.sleep(backoff * attempt + random.random())
    return "" # Should not be reached

# ------------------- Endpoint Functions (config 기반으로 수정) -------------------
def get_cate_items(session, cate_no: str, cfg, logger) -> List[Dict[str, str]]:
    url = urljoin(cfg["requests"]["base"], cfg["requests"]["ajax_cate_list"])
    html = http_request(session, "post", url, cfg, data={"cateNo": cate_no})
    
    soup = BeautifulSoup(html, "html.parser")
    items = []
    sel = cfg["selectors"]
    
    for btn in soup.select(sel["cate_button"]):
        label_src = sel.get("cate_button_label", "text")
        if label_src == "text":
            label = " ".join((btn.get_text() or "").strip().split())
        elif label_src.startswith("attr:"):
            label = btn.get(label_src.split(":", 1)[1], "").strip()
        else:
            label = " ".join((btn.get_text() or "").strip().split())

        sub = btn.get(sel["cate_button_sub_attr"], "").strip()
        if label and sub:
            items.append({"label": label, "cateSubNo": sub})
    return items

def get_sub_fragment(session, cate_no: str, cate_sub_no: str, cfg, logger):
    url = urljoin(cfg["requests"]["base"], cfg["requests"]["ajax_sub_list"])
    return http_request(session, "post", url, cfg, data={"cateNo": cate_no, "cateSubNo": cate_sub_no})

# ------------------- Main Fetcher Logic -------------------
def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()

def run_fetcher(**context):
    """
    Airflow DAG에서 호출할 Fetcher 메인 함수.
    웹사이트에서 HTML을 가져와 S3에 업로드합니다.
    """
    config_path = '/opt/airflow/config/official_fee/gongimnara.yaml'
    with open(config_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)

    # --- ✨ 수정된 부분 ---
    # Airflow의 표준 로거를 사용하도록 변경합니다.
    # 이렇게 하면 FileNotFoundError가 발생하지 않고, 로그가 Airflow UI에 자동으로 기록됩니다.
    logger = logging.getLogger("airflow.task")
    # --------------------

    session = requests.Session()

    # Airflow Connection을 사용하여 S3 Hook 생성
    s3_hook = S3Hook(aws_conn_id=cfg['aws']['s3_connection_id'])
    
    # ✨ 수정된 부분: config 파일에 raw_bucket_name 키가 없으므로 bucket_name을 사용합니다.
    # 이 부분은 사용하시는 config.yaml 파일의 키 이름에 맞춰주세요.
    # 만약 raw/out 버킷이 다르다면 fetcher는 raw_bucket_name을 사용해야 합니다.
    bucket_name = cfg['aws'].get('raw_bucket_name', cfg['aws'].get('bucket_name'))
    
    logger.info(f"S3 Bucket: {bucket_name}에 HTML 파일을 업로드합니다.")

    # Warm-up
    http_request(session, "get", urljoin(cfg["requests"]["base"], cfg["requests"]["page_url"]), cfg)

    for cate_no, cate_name in cfg["targets"]["cate"].items():
        logger.info(f"'{cate_name}' 카테고리 항목을 가져오는 중...")
        items = get_cate_items(session, cate_no, cfg, logger)
        
        total = len(items)
        for i, item in enumerate(items, 1):
            label, sub_no = item["label"], item["cateSubNo"]
            safe_label = sanitize_filename(label)
            
            # S3에 저장할 경로(Key) 설정
            s3_key = f"{cfg['aws']['raw_html_prefix']}/{sanitize_filename(cate_name)}/{safe_label}.html"

            try:
                logger.info(f"[{cate_name} ({i}/{total})] '{label}' 데이터 가져오는 중...")
                fragment_html = get_sub_fragment(session, cate_no, sub_no, cfg, logger)
                
                # S3에 문자열 직접 업로드
                s3_hook.load_string(
                    string_data=fragment_html,
                    key=s3_key,
                    bucket_name=bucket_name,
                    replace=True
                )
                
                logger.info(f" -> 성공적으로 's3://{bucket_name}/{s3_key}'에 저장했습니다.")
            except Exception as e:
                logger.error(f"'{label}' 처리 중 오류 발생: {e}", exc_info=True)
            
            time.sleep(random.uniform(cfg["requests"]["delay_min"], cfg["requests"]["delay_max"]))
    
    logger.info("[FETCHER COMPLETE] 모든 HTML 파일의 S3 업로드를 완료했습니다.")

# 로컬 테스트용
if __name__ == "__main__":
    run_fetcher()