import os
import random
import time
import logging
from typing import Dict, List, Optional, Generator, Tuple
from urllib.parse import urlencode

import requests
import yaml


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(name: str, log_file: Optional[str]) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        if log_file:
            os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
            logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(sh)
    return logger

_proxy_idx = 0
def pick_proxy(proxies: List[Optional[str]]) -> Optional[Dict[str, str]]:
    global _proxy_idx
    if not proxies or not any(proxies):
        return None
    p = proxies[_proxy_idx % len(proxies)]
    _proxy_idx += 1
    return {"http": p, "https": p} if p else None

def fetch_page(url: str, cfg: dict, logger: logging.Logger) -> Optional[str]:
    timeout = cfg["requests"]["timeout"]
    retries = cfg["requests"]["retries"]
    delay = cfg["requests"]["backoff_base_sec"]
    headers = (cfg["requests"].get("headers") or {}).copy()
    user_agents = cfg["requests"].get("user_agents") or []

    for attempt in range(1, retries + 1):
        headers["User-Agent"] = random.choice(user_agents) if user_agents else "Mozilla/5.0"
        proxies = pick_proxy(cfg["requests"].get("proxies") or [])
        try:
            resp = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            resp.raise_for_status()
            if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "ascii"):
                resp.encoding = resp.apparent_encoding
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"[RETRY {attempt}/{retries}] Failed to fetch {url}. Error: {e}")
            if attempt == retries:
                logger.error(f"[FAILED] Could not fetch {url} after {retries} attempts.")
                return None
            time.sleep(delay)
            delay *= cfg["requests"]["backoff_base_sec"]
    return None

def build_s3_key(prefix: str, category_name: str, page: int) -> str:
    safe_category = category_name.replace("/", "_")
    prefix = (prefix or "").strip("/")
    return f"{prefix}/{safe_category}/page_{page}.html" if prefix else f"{safe_category}/page_{page}.html"

def iter_pages(config_path: str) -> Generator[Tuple[str, str, dict], None, None]:
    """
    설정을 읽고 (key, html, meta) 튜플을 yield.
    - key: S3 키 또는 로컬 파일 경로 생성에 사용할 상대 키
    - html: 페이지 HTML
    - meta: 부가 메타데이터 (url, category, page 등)
    """
    cfg = load_config(config_path)
    logger = setup_logger("hyunki_downloader", cfg["general"].get("downloader_log_file"))

    base_url = cfg["site"]["base"]
    per_page_min = cfg["delays"]["per_page_min_sec"]
    per_page_max = cfg["delays"]["per_page_max_sec"]
    pause_between_categories = cfg["site"].get("pause_between_categories_sec", 0)
    prefix = (cfg.get("s3", {}) or {}).get("prefix", "")

    logger.info("=== HTML Downloader Started (generator) ===")

    for endpoint in cfg["site"]["endpoints"]:
        category_name = endpoint.strip("/").split("/")[0]
        logger.info(f"--- Processing category: {category_name} ({endpoint}) ---")

        for page in range(cfg["site"]["start_page"], cfg["site"]["max_pages"] + 1):
            url = f"{base_url}{endpoint}?{urlencode({cfg['site']['page_param']: page})}"
            html = fetch_page(url, cfg, logger)
            if not html:
                logger.warning(f"Stopping category '{category_name}' due to fetch failure at page {page}.")
                break

            key = build_s3_key(prefix, category_name, page)
            meta = {"source_url": url, "category": category_name, "page": page}

            yield key, html, meta

            time.sleep(random.uniform(per_page_min, per_page_max))

        logger.info(f"--- Finished category: {category_name} ---")
        if pause_between_categories > 0:
            time.sleep(pause_between_categories)

    logger.info("=== HTML Downloader Finished (generator) ===")
