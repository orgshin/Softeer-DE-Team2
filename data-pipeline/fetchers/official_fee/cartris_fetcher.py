import os
import re
import time
import random
import logging
import requests
from typing import Dict, Any, List
import yaml

# ==============================================================================
# 설정 및 로깅
# ==============================================================================

def load_config(path: str = "config.yaml") -> Dict[str, Any]:
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
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(logging.StreamHandler()) # 콘솔 출력도 추가
    return logger

# ==============================================================================
# URL 및 HTTP 요청 관리
# ==============================================================================

def build_urls(cfg: Dict[str, Any]) -> List[str]:
    """설정 파일의 ranges와 explicit 항목을 기반으로 전체 URL 목록을 생성합니다."""
    base = cfg["requests"]["base_url"]
    urls = []
    for r in cfg["urls"].get("ranges", []):
        for i in range(int(r["id_start"]), int(r["id_end"]) + 1):
            urls.append(f"{base}?type=&ca={r['ca']}&id={i}")
    for e in cfg["urls"].get("explicit", []):
        urls.append(f"{base}?type=&ca={e['ca']}&id={e['id']}")
    return urls

def fetch_html(url: str, cfg: Dict[str, Any], logger: logging.Logger) -> str:
    """
    단일 URL의 HTML 콘텐츠를 가져옵니다. 재시도 및 백오프 로직을 포함합니다.
    """
    req_cfg = cfg["requests"]
    headers = (req_cfg.get("headers") or {}).copy()
    user_agents = req_cfg.get("user_agents") or ["Mozilla/5.0"]
    headers["User-Agent"] = random.choice(user_agents)

    # 간단한 세션 사용. 프록시가 필요하다면 여기에 추가
    session = requests.Session()
    proxies = req_cfg.get("proxies") or []
    if proxies:
        proxy = random.choice(proxies)
        session.proxies = {"http": proxy, "https": proxy}

    for attempt in range(1, req_cfg["retries"] + 1):
        try:
            resp = session.get(url, headers=headers, timeout=req_cfg["timeout"])
            resp.raise_for_status()
            # 웹사이트의 실제 인코딩에 맞춰 보정
            if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "ascii"):
                resp.encoding = resp.apparent_encoding
            return resp.text
        except requests.RequestException as e:
            logger.warning(f"RETRY {attempt}/{req_cfg['retries']} | URL: {url} | Error: {e}")
            if attempt == req_cfg["retries"]:
                logger.error(f"FAILED to fetch {url} after {req_cfg['retries']} attempts.")
                return ""
            time.sleep(req_cfg["backoff_base"] * attempt)
    return ""

def get_filename_from_url(url: str) -> str:
    """URL에서 ca와 id를 추출하여 'ca_1_id_23.html' 같은 파일명을 생성합니다."""
    match_ca = re.search(r"ca=(\d+)", url)
    match_id = re.search(r"id=(\d+)", url)
    ca = match_ca.group(1) if match_ca else "unknown"
    id_ = match_id.group(1) if match_id else "unknown"
    return f"ca_{ca}_id_{id_}.html"

# ==============================================================================
# 메인 실행 로직
# ==============================================================================

def run_downloader():
    """설정에 따라 모든 HTML 페이지를 다운로드하여 로컬에 저장합니다."""
    cfg = load_config("data-pipeline/config/cartris.yaml")
    logger = setup_logger("downloader", cfg["general"]["downloader_log_file"])
    
    urls = build_urls(cfg)
    storage_dir = cfg["general"]["html_storage_dir"]
    os.makedirs(storage_dir, exist_ok=True)

    logger.info(f"=== HTML Downloader Started: {len(urls)} pages to fetch ===")

    for i, url in enumerate(urls, 1):
        filename = get_filename_from_url(url)
        filepath = os.path.join(storage_dir, filename)

        if os.path.exists(filepath):
            logger.info(f"[{i}/{len(urls)}] SKIP (already exists): {filepath}")
            continue

        logger.info(f"[{i}/{len(urls)}] FETCHING: {url}")
        html_content = fetch_html(url, cfg, logger)
        
        if html_content:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(html_content)
            logger.info(f"[{i}/{len(urls)}] SAVED to {filepath}")
        else:
            logger.warning(f"[{i}/{len(urls)}] FAILED to get content for {url}")
        
        # 서버 부하 감소를 위한 지연 시간
        time.sleep(random.uniform(cfg["requests"]["delay_min"], cfg["requests"]["delay_max"]))
    
    logger.info("=== HTML Downloader Finished ===")

if __name__ == "__main__":
    run_downloader()