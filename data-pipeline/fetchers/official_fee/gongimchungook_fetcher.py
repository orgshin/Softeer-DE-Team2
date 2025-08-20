import os
import time
import random
import logging
from typing import Dict, Optional

import requests
import yaml

# ==============================================================================
# 설정 및 로깅
# ==============================================================================

def load_config(path: str = "config.yaml") -> dict:
    """YAML 설정 파일을 로드합니다."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(name: str, log_file: str) -> logging.Logger:
    """지정된 이름과 파일 경로로 로거를 설정합니다."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        sh = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

# ==============================================================================
# HTTP 요청 관리
# ==============================================================================

def fetch_html(url: str, cfg: dict, logger: logging.Logger) -> Optional[str]:
    """단일 URL의 HTML 콘텐츠를 가져옵니다. 재시도 및 백오프 로직을 포함합니다."""
    req_cfg = cfg["requests"]
    headers = (req_cfg.get("headers") or {}).copy()
    user_agents = req_cfg.get("user_agents") or ["Mozilla/5.0"]
    headers["User-Agent"] = random.choice(user_agents)
    
    proxies = None
    if req_cfg.get("proxies"):
        p = random.choice(req_cfg["proxies"])
        proxies = {"http": p, "https": p}

    for attempt in range(1, req_cfg["retries"] + 1):
        try:
            resp = requests.get(
                url, headers=headers, proxies=proxies, timeout=req_cfg["timeout"]
            )
            resp.raise_for_status()
            if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "ascii"):
                resp.encoding = resp.apparent_encoding
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"RETRY {attempt}/{req_cfg['retries']} | URL: {url} | Error: {e}")
            if attempt == req_cfg["retries"]:
                logger.error(f"FAILED to fetch {url} after {req_cfg['retries']} attempts.")
                return None
            time.sleep(req_cfg["backoff_base_sec"] * attempt)
    return None

# ==============================================================================
# 메인 실행 로직
# ==============================================================================

def run_downloader():
    """설정에 따라 모든 HTML 페이지를 다운로드하여 로컬에 저장합니다."""
    cfg = load_config("config/gongimchungook.yaml")
    logger = setup_logger("downloader", cfg["general"]["downloader_log_file"])
    
    url_template = cfg["general"]["url_template"]
    numbers = cfg["general"]["numbers"]
    storage_dir = cfg["general"]["html_storage_dir"]
    os.makedirs(storage_dir, exist_ok=True)

    logger.info(f"=== HTML Downloader Started: {len(numbers)} pages to fetch ===")

    for i, number in enumerate(numbers, 1):
        url = url_template.format(number=number)
        filepath = os.path.join(storage_dir, f"page_{number}.html")

        if os.path.exists(filepath):
            logger.info(f"[{i}/{len(numbers)}] SKIP (already exists): {filepath}")
            continue

        logger.info(f"[{i}/{len(numbers)}] FETCHING: {url}")
        html_content = fetch_html(url, cfg, logger)
        
        if html_content:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html_content)
            logger.info(f"[{i}/{len(numbers)}] SAVED to {filepath}")
        
        # 다운로더는 페이지 간 딜레이를 주지 않음 (필요 시 추가)
    
    logger.info("=== HTML Downloader Finished ===")

if __name__ == "__main__":
    run_downloader()