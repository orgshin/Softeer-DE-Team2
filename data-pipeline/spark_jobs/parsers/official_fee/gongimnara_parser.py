import os
import re
import json
import glob
import yaml
import logging
from typing import Dict, Any, List, Tuple, Optional
from bs4 import BeautifulSoup


# --------------- Config & Logging ---------------
def load_cfg(path: str = "config.yaml") -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(path: str) -> logging.Logger:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    logger = logging.getLogger("gongim_parser")
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

def load_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()

def clean(s: str) -> str:
    return " ".join((s or "").strip().split())


# --------------- Filesystem Discovery ---------------
# cate 폴더명: "cate_<cateNo>__<cateNameSlug>"
CATE_DIR_RE = re.compile(r"^cate_(?P<no>\d+)(?:__(?P<name>.+))?$")
# sub 파일명:  "sub_<subNo>__<labelSlug>.html"
SUB_FILE_RE = re.compile(r"^sub_(?P<sub>\d+)(?:__(?P<label>.+))?\.html$")

def iter_sub_files(raw_dir: str) -> List[Dict[str, str]]:
    """manifest 없이 디스크만 순회하여 cate/sub 파일 경로와 메타를 수집."""
    results: List[Dict[str, str]] = []
    for cate_path in sorted(glob.glob(os.path.join(raw_dir, "cate_*"))):
        if not os.path.isdir(cate_path):
            continue
        m = CATE_DIR_RE.match(os.path.basename(cate_path))
        if not m:
            continue
        cate_no = m.group("no")
        cate_name = m.group("name") or ""

        for sub_path in sorted(glob.glob(os.path.join(cate_path, "sub_*.html"))):
            fn = os.path.basename(sub_path)
            sm = SUB_FILE_RE.match(fn)
            if not sm:
                continue
            sub_no = sm.group("sub")
            label = (sm.group("label") or "").rsplit(".", 1)[0]
            results.append({
                "cate_dir": cate_path,
                "cate_no": cate_no,
                "cate_name": cate_name,
                "sub_no": sub_no,
                "label": label,
                "file": sub_path,
            })
    return results


# --------------- Table → Matrix ---------------
def expand_table_to_matrix(table_tag, replicate_spans: bool) -> Tuple[List[List[str]], int, bool]:
    trs = table_tag.find_all("tr")
    if not trs:
        return [], 0, False

    matrix: List[List[str]] = []
    active_spans: List[Optional[Dict[str, Any]]] = []
    header_rows = set()
    has_header = False

    def fill_from_above(row: List[str], col_idx: int) -> int:
        while col_idx < len(active_spans) and active_spans[col_idx]:
            row.append(active_spans[col_idx]["text"])
            active_spans[col_idx]["remain"] -= 1
            if active_spans[col_idx]["remain"] <= 0:
                active_spans[col_idx] = None
            col_idx += 1
        return col_idx

    for tr in trs:
        tds = tr.find_all(["th", "td"])
        row, col_idx = [], 0
        col_idx = fill_from_above(row, col_idx)

        for cell in tds:
            text = clean(cell.get_text(separator=" "))
            rs = int(cell.get("rowspan") or 1)
            cs = int(cell.get("colspan") or 1)

            col_idx = fill_from_above(row, col_idx)
            for k in range(cs):
                row.append(text)
                if col_idx + k >= len(active_spans):
                    active_spans.extend([None] * (col_idx + k - len(active_spans) + 1))
                if replicate_spans and rs > 1:
                    active_spans[col_idx + k] = {"remain": rs - 1, "text": text}
            col_idx += cs
            col_idx = fill_from_above(row, col_idx)

        while col_idx < len(active_spans) and active_spans[col_idx]:
            row.append(active_spans[col_idx]["text"])
            active_spans[col_idx]["remain"] -= 1
            if active_spans[col_idx]["remain"] <= 0:
                active_spans[col_idx] = None
            col_idx += 1

        max_len = max((len(r) for r in matrix), default=0)
        if len(row) < max_len:
            row += [""] * (max_len - len(row))
        elif len(row) > max_len:
            for r in matrix:
                r += [""] * (len(row) - len(r))

        matrix.append(row)
        if tr.find("th"):
            header_rows.add(len(matrix) - 1)
            has_header = True

    header_idx = min(header_rows) if header_rows else 0
    return matrix, header_idx, has_header

def matrix_to_records(matrix: List[List[str]], header_idx: int, default_col_prefix: str, has_header: bool) -> List[Dict[str, str]]:
    if not matrix:
        return []

    ncols = max((len(r) for r in matrix), default=0)

    if not has_header:
        if ncols == 3:
            cols = ["타입", "설명", "금액시간"]
        elif ncols == 4:
            cols = ["구분1", "구분2", "설명", "금액시간"]
        else:
            cols = [f"{default_col_prefix}{i+1}" for i in range(ncols)]
        data_rows = matrix
    else:
        seen, cols = set(), []
        for i, h in enumerate(matrix[header_idx]):
            name = h if h else f"{default_col_prefix}{i+1}"
            while name in seen:
                name += "_1"
            seen.add(name)
            cols.append(name)
        data_rows = [r for i, r in enumerate(matrix) if i != header_idx]

    records: List[Dict[str, str]] = []
    for row in data_rows:
        cells = (row + [""] * (len(cols) - len(row)))[:len(cols)]
        rec = dict(zip(cols, cells))
        if any(cols) and all(rec[c] == c for c in cols if c.strip()):
            continue
        records.append(rec)
    return records


# --------------- Title Detection ---------------
def is_bad_title(text: str, cfg: dict) -> bool:
    if not text:
        return True
    t = text.strip()
    if t.startswith("*") or t.startswith("※"):
        return True
    if len(t) < cfg["titles"]["min_length"] or len(t) > cfg["titles"]["max_length"]:
        return True
    for kw in cfg["titles"]["exclude_keywords"]:
        if kw.lower() in t.lower():
            return True
    return False

def scan_prev_siblings(node, cfg) -> str:
    for sib in node.previous_siblings:
        name = getattr(sib, "name", None)
        if not name:
            continue
        for sel in cfg["titles"]["priority_selectors"]:
            found = None
            try:
                found = sib.select_one(sel) if hasattr(sib, "select_one") else None
            except Exception:
                found = None
            if found:
                t = clean(found.get_text())
                if not is_bad_title(t, cfg):
                    return t
        if name in ("div", "span", "b", "strong", "h3", "h4"):
            t = clean(sib.get_text())
            if not is_bad_title(t, cfg):
                return t
    return ""

def get_section_title(table_tag, cfg) -> str:
    title = scan_prev_siblings(table_tag, cfg)
    if title:
        return title
    parent = table_tag.parent
    for _ in range(cfg["titles"]["parent_hops"]):
        if not parent:
            break
        title = scan_prev_siblings(parent, cfg)
        if title:
            return title
        parent = parent.parent if hasattr(parent, "parent") else None
    return ""


# --------------- Labor Parsing ---------------
_PRICE_TIME_RE = re.compile(
    r"(?P<amount>\d[\d,]*)\s*원"
    r"(?:\s*\((?P<min>\d+)\s*[~\-]\s*(?P<max>\d+)\s*분\)|\s*\((?P<only>\d+)\s*분\))?"
)

def _parse_amount_time(s: str):
    m = _PRICE_TIME_RE.search(s or "")
    if not m:
        return None, None, None
    amt = int(m.group("amount").replace(",", ""))
    if m.group("only"):
        tmin = tmax = int(m.group("only"))
    else:
        tmin = int(m.group("min")) if m.group("min") else None
        tmax = int(m.group("max")) if m.group("max") else None
    return amt, tmin, tmax

def split_labor_amount_time(records: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in records:
        new_row: Dict[str, Any] = dict(row)
        for key in ("금액시간", "공임", "공임(원)", "공임비", "금액"):
            if key in row and row[key]:
                amt, tmin, tmax = _parse_amount_time(row[key])
                if amt is not None:
                    new_row["공임비"] = f"{amt}원"
                    new_row["소요시간_min"] = tmin
                    new_row["소요시간_max"] = tmax
                break
        out.append(new_row)
    return out


# --------------- Parse Fragment ---------------
def parse_fragment(html_fragment: str, cfg: dict) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_fragment, "html.parser")
    tables = soup.find_all("table")
    results: List[Dict[str, Any]] = []

    for t in tables:
        matrix, header_idx, has_header = expand_table_to_matrix(
            t, replicate_spans=bool(cfg["tables"]["replicate_spans"])
        )
        recs = matrix_to_records(
            matrix, header_idx,
            default_col_prefix=cfg["tables"]["default_col_prefix"],
            has_header=has_header
        )
        if not recs:
            continue
        recs = split_labor_amount_time(recs)
        title = get_section_title(t, cfg)
        results.append({"section_title": title, "records": recs})

    return results


# --------------- Runner ---------------
def main():
    cfg = load_cfg("config/gongimnara.yaml")
    logger = setup_logger(cfg["general"]["log_file"])

    raw_dir = cfg["general"]["raw_dir"]
    if not os.path.isdir(raw_dir):
        raise FileNotFoundError(f"raw_dir not found: {raw_dir} (먼저 downloader.py 실행 필요)")

    # cateNo -> cateName 매핑(폴더명에 이름이 없을 때 보강)
    cate_map: Dict[str, str] = cfg.get("targets", {}).get("cate", {}) or {}

    output: List[Dict[str, Any]] = []
    # cate_* 폴더 순회
    for cate_dir in sorted(glob.glob(os.path.join(raw_dir, "cate_*"))):
        if not os.path.isdir(cate_dir):
            continue
        m = re.match(r"^cate_(?P<no>\d+)(?:__(?P<name>.+))?$", os.path.basename(cate_dir))
        if not m:
            continue
        cate_no = m.group("no")
        cate_name = (m.group("name") or "").strip() or cate_map.get(cate_no, "")

        logger.info(f"[PARSE] cate {cate_no} - {cate_name or '(unknown)'}")

        bucket = {"cate_no": cate_no, "cate_name": cate_name, "items": []}
        sub_files = sorted(glob.glob(os.path.join(cate_dir, "sub_*.html")))
        if not sub_files:
            logger.info(f"  - no sub_*.html under {cate_dir}")
            output.append(bucket)
            continue

        for sub_path in sub_files:
            fn = os.path.basename(sub_path)
            sm = re.match(r"^sub_(?P<sub>\d+)(?:__(?P<label>.+))?\.html$", fn)
            sub_no = sm.group("sub") if sm else ""
            label = (sm.group("label") or "").rsplit(".", 1)[0] if sm else ""

            try:
                html = load_text(sub_path)
                sections = parse_fragment(html, cfg)
                bucket["items"].append({"label": label, "cateSubNo": sub_no, "sections": sections})
            except Exception as e:
                logger.exception(f"Failed parsing {sub_path}: {e}")

        output.append(bucket)

    out_path = cfg["general"]["out_json"]
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=cfg["general"]["pretty_indent"])

    logger.info(f"[DONE] Parsed JSON saved -> {out_path}")


if __name__ == "__main__":
    main()
