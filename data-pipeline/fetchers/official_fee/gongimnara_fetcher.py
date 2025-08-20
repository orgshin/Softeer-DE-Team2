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

def main():
    cfg = load_cfg("./config/official_fee/gongimnara.yaml")
    logger = setup_logger(cfg["general"]["log_file"])
    session = requests.Session()
    
    output_base_dir = cfg["general"]["raw_dir"]
    os.makedirs(output_base_dir, exist_ok=True)
    logger.info(f"HTML 파일을 저장할 기본 폴더: {output_base_dir}")

    http_request(session, "get", urljoin(cfg["requests"]["base"], cfg["requests"]["page_url"]), cfg)

    for cate_no, cate_name in cfg["targets"]["cate"].items():
        cate_dir = os.path.join(output_base_dir, sanitize_filename(cate_name))
        os.makedirs(cate_dir, exist_ok=True)
        
        logger.info(f"'{cate_name}' 카테고리 항목을 가져오는 중...")
        items = get_cate_items(session, cate_no, cfg, logger)
        
        total = len(items)
        for i, item in enumerate(items, 1):
            label, sub_no = item["label"], item["cateSubNo"]
            safe_label = sanitize_filename(label)
            output_path = os.path.join(cate_dir, f"{safe_label}.html")

            try:
                logger.info(f"[{cate_name} ({i}/{total})] '{label}' 데이터 가져오는 중...")
                fragment_html = get_sub_fragment(session, cate_no, sub_no, cfg, logger)
                
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(fragment_html)
                
                logger.info(f" -> 성공적으로 '{output_path}'에 저장했습니다.")
            except Exception as e:
                logger.error(f"'{label}' 처리 중 오류 발생: {e}", exc_info=True)
            
            time.sleep(random.uniform(cfg["requests"]["delay_min"], cfg["requests"]["delay_max"]))
    
    logger.info("[FETCHING COMPLETE] 모든 HTML 파일 저장을 완료했습니다.")

if __name__ == "__main__":
    main()