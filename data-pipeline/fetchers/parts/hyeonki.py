import os
import random
import time
import logging
from typing import Dict, List, Optional
from urllib.parse import urlencode

import requests
import yaml

# ==============================================================================
# 설정 및 로깅 (공통 모듈로 분리 가능)
# ==============================================================================

def load_config(path: str = "config.yaml") -> dict:
    """YAML 설정 파일을 로드합니다."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(name: str, log_file: str) -> logging.Logger:
    """로거를 설정합니다."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        # 파일 핸들러와 스트림 핸들러 설정
        fh = logging.FileHandler(log_file, encoding="utf-8")
        sh = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

# ==============================================================================
# HTTP 요청 헬퍼
# ==============================================================================

_proxy_idx = 0

def pick_proxy(proxies: List[Optional[str]]) -> Optional[Dict[str, str]]:
    """프록시 목록에서 순차적으로 하나를 선택합니다."""
    global _proxy_idx
    if not proxies or not any(proxies):
        return None
    p = proxies[_proxy_idx % len(proxies)]
    _proxy_idx += 1
    return {"http": p, "https": p} if p else None

def fetch_page(url: str, cfg: dict, logger: logging.Logger) -> Optional[str]:
    """
    지정된 URL의 HTML 콘텐츠를 가져옵니다.
    실패 시 설정에 따라 재시도합니다.
    """
    timeout = cfg["requests"]["timeout"]
    retries = cfg["requests"]["retries"]
    delay = cfg["requests"]["backoff_base_sec"]
    headers = (cfg["requests"].get("headers") or {}).copy()
    user_agents = cfg["requests"].get("user_agents") or []

    for attempt in range(1, retries + 1):
        # 매 시도마다 User-Agent와 프록시를 변경
        headers["User-Agent"] = random.choice(user_agents) if user_agents else "Mozilla/5.0"
        proxies = pick_proxy(cfg["requests"].get("proxies") or [])
        
        try:
            resp = requests.get(url, headers=headers, proxies=proxies, timeout=timeout)
            resp.raise_for_status()
            # 웹사이트의 실제 인코딩에 맞춰 보정
            if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "ascii"):
                resp.encoding = resp.apparent_encoding
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"[RETRY {attempt}/{retries}] Failed to fetch {url}. Error: {e}")
            if attempt == retries:
                logger.error(f"[FAILED] Could not fetch {url} after {retries} attempts.")
                return None
            time.sleep(delay)
            delay *= cfg["requests"]["backoff_base_sec"] # Exponential backoff
    return None

# ==============================================================================
# 다운로더 메인 로직
# ==============================================================================

def run_downloader():
    """설정 파일에 정의된 모든 페이지를 다운로드하여 로컬에 저장합니다."""
    cfg = load_config("data-pipeline/config/hyeonki.yaml")
    logger = setup_logger("downloader", cfg["general"]["downloader_log_file"])
    
    base_url = cfg["site"]["base"]
    output_dir = cfg["general"]["html_output_dir"]

    logger.info("=== HTML Downloader Started ===")

    for endpoint in cfg["site"]["endpoints"]:
        # Endpoint에서 카테고리 이름을 추출하여 폴더명으로 사용 (e.g., /엔진/50/ -> 엔진)
        category_name = endpoint.strip("/").split("/")[0]
        category_dir = os.path.join(output_dir, category_name)
        os.makedirs(category_dir, exist_ok=True)
        
        logger.info(f"--- Processing category: {category_name} ({endpoint}) ---")

        for page in range(cfg["site"]["start_page"], cfg["site"]["max_pages"] + 1):
            query_params = urlencode({cfg["site"]["page_param"]: page})
            url = f"{base_url}{endpoint}?{query_params}"
            
            # 파일 저장 경로 설정
            file_path = os.path.join(category_dir, f"page_{page}.html")
            
            if os.path.exists(file_path):
                logger.info(f"[SKIP] Already downloaded: {file_path}")
                continue

            html_content = fetch_page(url, cfg, logger)
            if html_content:
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(html_content)
                logger.info(f"[SAVED] {url} -> {file_path}")
            else:
                # 페이지 다운로드 실패 시 해당 카테고리는 중단하고 다음으로 넘어감
                logger.warning(f"Stopping category '{category_name}' due to fetch failure at page {page}.")
                break
            
            # 서버 부하를 줄이기 위한 지연
            delay = random.uniform(cfg["delays"]["per_page_min_sec"], cfg["delays"]["per_page_max_sec"])
            time.sleep(delay)

        logger.info(f"--- Finished category: {category_name} ---")
        time.sleep(cfg["delays"]["between_categories_sec"])

    logger.info("=== HTML Downloader Finished ===")

if __name__ == "__main__":
    run_downloader()