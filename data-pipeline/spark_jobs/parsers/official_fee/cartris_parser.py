import os
import re
import json
import logging
import glob
from typing import Dict, Any, List

import yaml
from bs4 import BeautifulSoup, Tag

# ==============================================================================
# 설정 및 로깅
# ==============================================================================

def load_config(path: str = "config.yaml") -> Dict[str, Any]:
    """YAML 설정 파일을 로드합니다."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(name: str, log_file: str) -> logging.Logger:
    """로거를 설정합니다."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        fh = logging.FileHandler(log_file, mode="w", encoding="utf-8")
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(logging.StreamHandler())
    return logger

# ==============================================================================
# 파싱 헬퍼 및 메인 로직 (원본 코드 로직 복원)
# ==============================================================================

def clean_text(text: str) -> str:
    """문자열의 불필요한 공백을 제거하고 정리합니다."""
    return " ".join((text or "").strip().split())

def split_labor_and_time(row: Dict[str, str], cfg: Dict[str, Any]) -> Dict[str, str]:
    """'공임비' 컬럼을 찾아 금액과 '소요시간'으로 분리하는 원본 기능입니다."""
    labor_pat = re.compile(cfg["parsing"]["labor_col_regex"])
    amount_time_re = re.compile(cfg["parsing"]["amount_time_regex"])

    labor_col = next((k for k in row if labor_pat.search(k)), None)
    if not labor_col:
        return row

    raw_value = row.get(labor_col, "")
    match = amount_time_re.search(raw_value)
    
    # 원본 JSON 파일에 맞게 '공임비'와 '소요시간'으로 컬럼명을 고정합니다.
    if match:
        amount = match.group("amount")
        time = (match.group("time") or "").strip()
        row['공임비'] = f"{amount}원" if amount else ""
        row['소요시간'] = time
    else:
        row['공임비'] = raw_value
        row.setdefault('소요시간', "")
    
    # 원본 컬럼이 '공임비'가 아니었다면 삭제
    if labor_col != '공임비':
        del row[labor_col]
        
    return row

def parse_table_records(table: Tag, cfg: Dict[str, Any]) -> List[Dict[str, str]]:
    """HTML 테이블을 파싱하여 레코드 리스트를 반환하는 원본 기능입니다."""
    headers = []
    thead = table.select_one("thead")
    if thead:
        header_rows = thead.select("tr")
        if header_rows:
            target_row = header_rows[-1] if cfg["selectors"].get("header_last_row") else header_rows[0]
            headers = [clean_text(th.get_text()) for th in target_row.select("th, td")]

    records = []
    tbody = table.select_one("tbody")
    if not tbody:
        return records
    
    if not headers:
        first_tr = tbody.select_one("tr")
        if not first_tr: return []
        num_cols = len(first_tr.select("td, th"))
        prefix = cfg["parsing"].get("default_col_prefix", "col_")
        headers = [f"{prefix}{i+1}" for i in range(num_cols)]

    for tr in tbody.select("tr"):
        cells = [clean_text(td.get_text()) for td in tr.select("td, th")]
        if len(cells) < len(headers):
            cells.extend([""] * (len(headers) - len(cells)))
        
        row_data = dict(zip(headers, cells[:len(headers)]))
        processed_row = split_labor_and_time(row_data, cfg)
        records.append(processed_row)
        
    return records

def parse_html_to_json_structure(html: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    """HTML 콘텐츠를 파싱하여 원본과 동일한 계층적 JSON 구조로 만듭니다."""
    soup = BeautifulSoup(html, "html.parser")
    sel_cfg = cfg["selectors"]
    
    page_category_tag = soup.select_one(sel_cfg["page_category"])
    page_category = clean_text(page_category_tag.get_text()) if page_category_tag else ""

    tables_dict: Dict[str, List[Dict[str, str]]] = {}
    for section in soup.select(sel_cfg["section"]):
        table_title_tag = section.select_one(sel_cfg["table_title"])
        table_title = clean_text(table_title_tag.get_text()) if table_title_tag else "기타"
        
        all_records_in_section: List[Dict[str, str]] = []
        for table in section.select(sel_cfg["table"]):
            all_records_in_section.extend(parse_table_records(table, cfg))
        
        if not all_records_in_section:
            continue
            
        if table_title in tables_dict:
            tables_dict[table_title].extend(all_records_in_section)
        else:
            tables_dict[table_title] = all_records_in_section
            
    return {"page_category": page_category, "tables": tables_dict}


# ==============================================================================
# 메인 실행 로직
# ==============================================================================

def run_parser():
    """로컬 HTML 파일들을 파싱하여 하나의 최종 JSON 파일로 저장합니다."""
    cfg = load_config("config/cartris.yaml")
    logger = setup_logger("parser", cfg["general"]["parser_log_file"])
    
    html_dir = cfg["general"]["html_storage_dir"]
    html_files = glob.glob(os.path.join(html_dir, "*.html"))

    if not html_files:
        logger.warning(f"No HTML files found in '{html_dir}'. Run downloader first.")
        return

    logger.info(f"=== JSON Parser Started: {len(html_files)} files to parse ===")
    
    all_pages_data = []
    for i, filepath in enumerate(html_files, 1):
        logger.info(f"[{i}/{len(html_files)}] PARSING: {os.path.basename(filepath)}")
        with open(filepath, "r", encoding="utf-8") as f:
            html_content = f.read()
        
        # 각 HTML 파일을 파싱하여 페이지별 딕셔너리 생성
        page_data = parse_html_to_json_structure(html_content, cfg)
        all_pages_data.append(page_data)

    # 모든 페이지 데이터를 리스트에 담아 하나의 JSON 파일로 저장
    output_path = cfg["general"]["output_json"]
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_pages_data, f, ensure_ascii=False, indent=cfg["general"].get("pretty_indent"))
        
    logger.info(f"SUCCESS: {len(all_pages_data)} pages of data saved to {output_path}")
    logger.info("=== JSON Parser Finished ===")

if __name__ == "__main__":
    run_parser()