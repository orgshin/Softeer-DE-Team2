import os
import re
import json
import time
import random
import logging
from typing import Dict, Any, List, Optional
from urllib.parse import urljoin

import requests
import yaml
from bs4 import BeautifulSoup


# ---------------- Config & Logging ----------------
def load_cfg(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    logger = logging.getLogger("gongim_downloader")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(path, encoding="utf-8")
        sh = logging.StreamHandler()
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        fh.setFormatter(fmt)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger


# ---------------- HTTP Utilities ----------------
def _pick_ua(cfg: dict) -> str:
    uas = cfg["requests"].get("user_agents") or []
    return random.choice(uas) if uas else "Mozilla/5.0"

def _new_session(cfg: dict) -> requests.Session:
    s = requests.Session()
    proxies = cfg["requests"].get("proxies") or []
    if proxies:
        p = random.choice(proxies)
        s.proxies = {"http": p, "https": p}
    return s

def _fix_encoding(resp: requests.Response) -> str:
    if not resp.encoding or resp.encoding.lower() in ("iso-8859-1", "ascii"):
        resp.encoding = resp.apparent_encoding
    return resp.text

def http_get(session: requests.Session, url: str, cfg: dict, logger: logging.Logger) -> str:
    timeout = cfg["requests"]["timeout"]
    retries = cfg["requests"]["retries"]
    backoff = float(cfg["requests"]["backoff_base"])
    headers = (cfg["requests"].get("headers") or {}).copy()
    headers["User-Agent"] = _pick_ua(cfg)
    headers["Referer"] = urljoin(cfg["requests"]["base"], cfg["requests"]["page_url"])
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return _fix_encoding(r)
        except requests.RequestException as e:
            if attempt == retries:
                logger.error(f"GET failed {url} -> {e}")
                raise
            time.sleep(backoff * attempt + random.random())

def http_post(session: requests.Session, url: str, data: dict, cfg: dict, logger: logging.Logger) -> str:
    timeout = cfg["requests"]["timeout"]
    retries = cfg["requests"]["retries"]
    backoff = float(cfg["requests"]["backoff_base"])
    headers = (cfg["requests"].get("headers") or {}).copy()
    headers["User-Agent"] = _pick_ua(cfg)
    headers["Referer"] = urljoin(cfg["requests"]["base"], cfg["requests"]["page_url"])
    for attempt in range(1, retries + 1):
        try:
            r = session.post(url, headers=headers, data=data, timeout=timeout)
            r.raise_for_status()
            return _fix_encoding(r)
        except requests.RequestException as e:
            if attempt == retries:
                logger.error(f"POST failed {url} -> {e}")
                raise
            time.sleep(backoff * attempt + random.random())


# ---------------- Helpers ----------------
def clean(s: str) -> str:
    return " ".join((s or "").strip().split())

_SAFE_CHARS_RE = re.compile(r"[^0-9A-Za-z가-힣._ -]+")

def slugify(s: str, max_len: int = 80) -> str:
    """파일/폴더명으로 안전하게 변환(한글 유지, 금지문자 제거)."""
    s = clean(s)
    s = _SAFE_CHARS_RE.sub("_", s)
    return s[:max_len].rstrip(" ._-")

def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)

def save_text(path: str, text: str) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


# ---------------- Extraction (no parsing) ----------------
def get_cate_items(session: requests.Session, cate_no: str, cfg: dict, logger: logging.Logger) -> List[Dict[str, str]]:
    """AJAX cate 리스트에서 하위 버튼(label, cateSubNo)만 추출."""
    url = urljoin(cfg["requests"]["base"], cfg["requests"]["ajax_cate_list"])
    html = http_post(session, url, {"cateNo": cate_no}, cfg, logger)
    soup = BeautifulSoup(html, "html.parser")

    sel = cfg["selectors"]
    items: List[Dict[str, str]] = []
    for btn in soup.select(sel["cate_button"]):
        # 라벨 소스: text 또는 attr:xxx
        label_src = sel.get("cate_button_label", "text")
        if label_src == "text":
            label = clean(btn.get_text())
        elif label_src.startswith("attr:"):
            label = clean(btn.get(label_src.split(":", 1)[1]) or "")
        else:
            label = clean(btn.get_text())

        sub = btn.get(sel["cate_button_sub_attr"]) or ""
        if sub:
            items.append({"label": label, "cateSubNo": sub})
    return items


# ---------------- Main ----------------
def main():
    cfg = load_cfg("config/gongimnara.yaml")
    logger = setup_logger(cfg["general"]["log_file"])
    raw_dir = cfg["general"]["raw_dir"]
    ensure_dir(raw_dir)

    session = _new_session(cfg)

    # Warm-up (쿠키/리퍼러)
    warm_url = urljoin(cfg["requests"]["base"], cfg["requests"]["page_url"])
    http_get(session, warm_url, cfg, logger)

    for cate_no, cate_name in cfg["targets"].get("cate", {}).items():
        cate_name_slug = slugify(cate_name) if cate_name else "unknown"
        cate_dir = os.path.join(raw_dir, f"cate_{cate_no}__{cate_name_slug}")
        ensure_dir(cate_dir)

        logger.info(f"[CATE] {cate_no} - {cate_name}")
        # 하위 목록 HTML 저장(디버깅/재파싱용)
        list_html = http_post(
            session,
            urljoin(cfg["requests"]["base"], cfg["requests"]["ajax_cate_list"]),
            {"cateNo": cate_no},
            cfg, logger
        )
        save_text(os.path.join(cate_dir, "cate_list.html"), list_html)

        # 서브 항목 HTML 저장
        items = get_cate_items(session, cate_no, cfg, logger)
        for i, it in enumerate(items, 1):
            sub_no = it["cateSubNo"]
            label = it["label"]
            label_slug = slugify(label)

            logger.info(f"  - [{i}/{len(items)}] sub={sub_no} label={label}")

            frag_html = http_post(
                session,
                urljoin(cfg["requests"]["base"], cfg["requests"]["ajax_sub_list"]),
                {"cateNo": cate_no, "cateSubNo": sub_no},
                cfg, logger
            )

            # 파일명 규칙: sub_<subNo>__<labelSlug>.html
            out_path = os.path.join(cate_dir, f"sub_{sub_no}__{label_slug}.html")
            save_text(out_path, frag_html)

            time.sleep(random.uniform(cfg["requests"]["delay_min"], cfg["requests"]["delay_max"]))

    logger.info("[DONE] Download complete (no manifest).")


if __name__ == "__main__":
    main()
