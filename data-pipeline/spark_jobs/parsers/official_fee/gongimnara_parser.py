# parser.py

import os
import re
import csv
import glob
import yaml
import logging
from bs4 import BeautifulSoup, Tag
from typing import Dict, Any, List, Optional, Tuple

# ------------------- Config & Logging -------------------
def load_cfg(path: str = "gongimnara.yaml") -> Dict[str, Any]:
    # 실제 환경에 맞는 config 경로를 사용하세요.
    # 예: "config/official_fee/gongimnara.yaml"
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def setup_logger(log_path: str) -> logging.Logger:
    logger = logging.getLogger("gongim_parser")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        sh = logging.StreamHandler()
        fh = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        fh.setFormatter(formatter); sh.setFormatter(formatter)
        logger.addHandler(fh); logger.addHandler(sh)
    return logger

# ------------------- Parsing Helpers (강화) -------------------
def clean(s: str) -> str:
    return " ".join((s or "").strip().split())

def is_bad_title(text: str, cfg) -> bool:
    if not text: return True
    t = text.strip()
    rules = cfg["titles"]
    if len(t) < rules["min_length"] or len(t) > rules["max_length"]: return True
    for kw in rules["exclude_keywords"]:
        if kw.lower() in t.lower(): return True
    # '체크사항'과 같은 테이블은 제목으로 보지 않음
    if '체크사항' in t: return True
    return False

def get_section_title(table_tag, cfg) -> str:
    # ... (이전 코드와 동일, 변경 없음) ...
    parent = table_tag
    for _ in range(cfg["titles"]["parent_hops"] + 1):
        if not parent: break
        for sib in parent.previous_siblings:
            if not hasattr(sib, "name"): continue
            for sel in cfg["titles"]["priority_selectors"]:
                try:
                    found = sib.select_one(sel) if hasattr(sib, "select_one") else None
                    if found:
                        t = clean(found.get_text())
                        if not is_bad_title(t, cfg): return t
                except Exception: continue
            if sib.name in ["div", "span", "b", "strong", "h3", "h4"]:
                t = clean(sib.get_text())
                if not is_bad_title(t, cfg): return t
        parent = parent.parent
    return ""

def _parse_title_and_time(title_text: str) -> Tuple[str, Optional[int], Optional[int]]:
    # ... (이전 코드와 동일, 변경 없음) ...
    time_pattern = re.compile(r"약?\s*(\d+)\s*[~-]\s*(\d+)\s*시간")
    match = time_pattern.search(title_text)
    tmin, tmax, clean_title = None, None, title_text
    if match:
        tmin, tmax = int(match.group(1)) * 60, int(match.group(2)) * 60
        clean_title = time_pattern.sub("", title_text).strip()
    return clean(clean_title), tmin, tmax

def parse_time_str(time_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not time_str: return None, None
    # "15~20분" 또는 "(20분)" 같은 형태를 처리
    match = re.search(r'(\d+)(?:~)?(\d+)?', time_str)
    if match:
        g = match.groups()
        tmin = int(g[0])
        tmax = int(g[1]) if g[1] else tmin
        return tmin, tmax
    return None, None

def expand_table_to_matrix(table_tag: Tag) -> List[List[str]]:
    """
    rowspan과 colspan을 처리하여 HTML 테이블을 2D 리스트(matrix)로 변환합니다.
    """
    matrix = []
    # active_spans는 rowspan을 처리하기 위한 상태 저장 변수
    # 형식: [{'remain': 남은 row 수, 'text': 셀 텍스트}]
    active_spans = []

    for row_idx, tr in enumerate(table_tag.find_all('tr')):
        row_out = []
        col_idx = 0

        # 위에서 내려온 rowspan 셀부터 채우기
        for span in list(active_spans):
            if span['row_idx'] + span['rowspan'] <= row_idx:
                active_spans.remove(span)
                continue
            while col_idx < span['col_idx']:
                # 이전 rowspan이 끝났는지 확인
                ended_span = next((s for s in active_spans if s['col_idx'] == col_idx and s['row_idx'] + s['rowspan'] <= row_idx), None)
                if not ended_span:
                    row_out.append("")
                col_idx += 1

            row_out.insert(span['col_idx'], span['text'])
            col_idx = span['col_idx'] + 1


        # 현재 row의 실제 셀들 처리
        for td in tr.find_all(['td', 'th']):
            while True:
                is_spanned = any(s['col_idx'] == col_idx for s in active_spans)
                if not is_spanned:
                    break
                col_idx += 1

            rs = int(td.get('rowspan', 1))
            cs = int(td.get('colspan', 1))
            text = clean(td.get_text(separator=' '))

            if rs > 1:
                active_spans.append({'rowspan': rs, 'colspan': cs, 'text': text, 'row_idx': row_idx, 'col_idx': col_idx})

            for _ in range(cs):
                row_out.append(text)
                col_idx += 1
        matrix.append(row_out)

    # 행의 길이를 맞추기 위해 빈 문자열 추가
    if not matrix: return []
    max_cols = max(len(r) for r in matrix)
    for r in matrix:
        r.extend([""] * (max_cols - len(r)))

    return matrix

def parse_fragment(html_fragment: str, cfg) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_fragment, "html.parser")
    output_sections = []
    
    for table in soup.find_all("table"):
        raw_title = get_section_title(table, cfg)
        if not raw_title: continue # 제목 없는 테이블(체크사항 등) 건너뛰기

        section_title, tmin_title, tmax_title = _parse_title_and_time(raw_title)
        
        matrix = expand_table_to_matrix(table)
        if not matrix: continue

        header = [clean(h) for h in matrix[0]]
        data_rows = matrix[1:]

        records = []
        for row in data_rows:
            # 3열 테이블 (구분, 작업종류, 공임비)
            if len(row) >= 3 and row[0] and row[1]:
                division = row[0]
                job_detail = row[1]
                price_field = row[2]

                # '구분'과 '작업종류'를 조합하여 더 명확한 작업 내용 생성
                if division == job_detail: # '엔진플러싱' 테이블처럼 내용이 같은 경우
                     job_combined = division
                else:
                     job_combined = f"{division} - {job_detail}"

                records.append({
                    "작업종류": job_combined,
                    "공임시간필드": price_field,
                    "소요시간_min_제목": tmin_title,
                    "소요시간_max_제목": tmax_title,
                })
            # 2열 테이블 (엔진플러싱 등)
            elif len(row) >= 2 and row[0] and row[1]:
                 records.append({
                    "작업종류": row[0],
                    "공임시간필드": row[1],
                    "소요시간_min_제목": tmin_title,
                    "소요시간_max_제목": tmax_title,
                })

        if records:
            output_sections.append({"section_title": section_title, "records": records})
    return output_sections

def to_row(rec: Dict[str, Any], cate_name: str, label: str, section_title: str, cfg) -> Optional[Dict[str, Any]]:
    # ... (이전 코드와 동일, 변경 없음) ...
    price_field = rec['공임시간필드']
    amount_time_re = re.compile(cfg['parsing']['amount_time_regex'])
    match = amount_time_re.match(price_field)

    fee_num, tmin, tmax = None, None, None
    if match:
        fee_num_str = match.group('amount')
        time_str = match.group('time')
        if fee_num_str: fee_num = int(fee_num_str.replace(',', ''))
        if time_str: tmin, tmax = parse_time_str(time_str)
    else:
        fee_only_match = re.search(r"[\d,]+", price_field)
        if fee_only_match: fee_num = int(fee_only_match.group(0).replace(',', ''))
        tmin, tmax = parse_time_str(price_field) # 가격 외 시간 정보만 따로 파싱

    if tmin is None: tmin = rec.get('소요시간_min_제목')
    if tmax is None: tmax = rec.get('소요시간_max_제목')

    return {
        "cate_name": cate_name, "label": label, "section_title": section_title,
        "공임비_num": fee_num, "소요시간_min": tmin, "소요시간_max": tmax,
        "작업종류": rec.get("작업종류"),
    }

# ------------------- Main Parser Logic -------------------
def main():
    # 실제 환경에 맞는 config 경로를 지정해주세요.
    cfg = load_cfg("./config/official_fee/gongimnara.yaml")
    logger = setup_logger(cfg["general"]["log_file"])
    
    input_base_dir = cfg["general"]["raw_dir"]
    # CSV 파일명은 config에 out_csv가 있으면 사용, 없으면 output.csv로 저장
    output_csv_path = cfg["general"].get("out_csv", "output.csv")

    html_files = glob.glob(os.path.join(input_base_dir, "**", "*.html"), recursive=True)
    if not html_files:
        logger.warning(f"'{input_base_dir}' 폴더에 파싱할 HTML 파일이 없습니다. fetcher.py를 먼저 실행해주세요.")
        return
        
    logger.info(f"총 {len(html_files)}개의 HTML 파일을 파싱합니다.")

    CSV_FIELDS = ["cate_name", "label", "section_title", "공임비_num", "소요시간_min", "소요시간_max", "작업종류"]
    
    with open(output_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        
        for file_path in html_files:
            try:
                parts = file_path.split(os.sep)
                cate_name = parts[-2]
                label = os.path.splitext(parts[-1])[0]
                logger.info(f"파싱 중: {file_path}")
                
                with open(file_path, "r", encoding="utf-8") as html_file:
                    html_content = html_file.read()
                
                sections = parse_fragment(html_content, cfg)
                
                for sec in sections:
                    section_title = sec.get("section_title", "")
                    for rec in sec.get("records", []):
                        row_data = to_row(rec, cate_name, label, section_title, cfg)
                        if row_data:
                            safe_row = {k: ("" if v is None else v) for k, v in row_data.items()}
                            writer.writerow(safe_row)
            except Exception as e:
                logger.error(f"'{file_path}' 파일 처리 중 오류 발생: {e}", exc_info=True)

    logger.info(f"[PARSING COMPLETE] 최종 결과가 '{output_csv_path}'에 저장되었습니다.")

if __name__ == "__main__":
    main()