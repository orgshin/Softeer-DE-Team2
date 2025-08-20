import os
import re
import csv
import logging
from typing import Dict, List, Optional

import yaml
from bs4 import BeautifulSoup
from pydantic import BaseModel, ValidationError

# ==============================================================================
# 데이터 유효성 검증을 위한 Pydantic 모델
# ==============================================================================

class PartItem(BaseModel):
    """
    크롤링한 부품 아이템의 데이터 구조를 정의하고 유효성을 검증합니다.
    """
    category: Optional[str] = None
    category_id: Optional[str] = None
    name: str
    part_code: Optional[str] = None
    price: int

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
        fh = logging.FileHandler(log_file, encoding="utf-8")
        sh = logging.StreamHandler()
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        fh.setFormatter(fmt)
        sh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.addHandler(sh)
    return logger

# ==============================================================================
# 파싱 헬퍼 함수
# ==============================================================================

def clean_text(text: str) -> str:
    """문자열의 불필요한 공백을 제거하고 정리합니다."""
    return re.sub(r"\s+", " ", text or "").strip()

def split_name_and_code(name: str, code_regex: str):
    """상품명에서 이름과 부품 코드를 분리합니다."""
    name = clean_text(name)
    match = re.search(code_regex, name)
    if match:
        part_name = clean_text(name[:match.start()])
        part_code = match.group(1).strip()
        return part_name, part_code
    return name, None

def parse_price(text: str, price_regex: str) -> Optional[int]:
    """가격 텍스트에서 숫자만 추출하여 정수형으로 변환합니다."""
    match = re.search(price_regex, text or "")
    return int(match.group(0).replace(",", "")) if match else None

def extract_visible_price_tag(li_tag: BeautifulSoup, cfg_selectors: dict):
    """
    여러 가격 후보 중 화면에 보이는(visible) 가격 태그를 우선적으로 찾습니다.
    'displaynone'과 같은 클래스가 없는 태그를 우선합니다.
    """
    hidden_class = cfg_selectors["hidden_class"]
    for selector in cfg_selectors["price_candidates"]:
        for candidate in li_tag.select(selector):
            if hidden_class not in (candidate.get("class") or []):
                return candidate
    # 보이는 가격이 없으면, 후보군 중 첫 번째 태그를 반환
    for selector in cfg_selectors["price_candidates"]:
        candidate = li_tag.select_one(selector)
        if candidate:
            return candidate
    return None

# ==============================================================================
# CSV 처리
# ==============================================================================

def write_to_csv(items: List[PartItem], file_path: str, fields: List[str]):
    """
    파싱 및 검증이 완료된 데이터를 CSV 파일에 추가합니다.
    파일이 없으면 헤더와 함께 새로 생성합니다.
    """
    file_exists = os.path.exists(file_path)
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)
    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        if not file_exists:
            writer.writeheader()
        
        for item in items:
            full_data = item.model_dump()
            row_to_write = {field: full_data.get(field) for field in fields}
            writer.writerow(row_to_write)

# ==============================================================================
# 파서 메인 로직
# ==============================================================================

def parse_html_file(html_content: str, cfg: dict, logger: logging.Logger) -> List[PartItem]:
    """단일 HTML 파일의 내용을 파싱하여 PartItem 리스트를 반환합니다."""
    soup = BeautifulSoup(html_content, "html.parser")
    cfg_sel = cfg["selectors"]
    cfg_parse = cfg["parsing"]

    # 목록 컨테이너가 없어도 전체 문서에서 검색 시도
    container = soup.select_one(cfg_sel["list_container"]) or soup
    
    validated_items: List[PartItem] = []
    
    for li in container.select(cfg_sel["item_li"]):
        # 이름 추출
        name_tag = next((li.select_one(sel) for sel in cfg_sel["name_candidates"] if li.select_one(sel)), None)
        if not name_tag:
            continue
        raw_name = name_tag.get_text(strip=True)
        name, part_code = split_name_and_code(raw_name, cfg_parse["part_code_regex"])

        # 가격 추출
        price_tag = extract_visible_price_tag(li, cfg_sel)
        price = parse_price(price_tag.get_text() if price_tag else "", cfg_parse["price_regex"])

        try:
            # Pydantic 모델을 사용하여 데이터 생성 및 유효성 검증
            item = PartItem(name=name, part_code=part_code, price=price)
            validated_items.append(item)
        except ValidationError as e:
            logger.warning(f"Data validation failed for item '{raw_name}'. Errors: {e.errors()}")
            
    return validated_items

def run_parser():
    """로컬에 저장된 모든 HTML 파일을 파싱하고 결과를 CSV로 저장합니다."""
    cfg = load_config("data-pipeline/config/hyunki.yaml")
    logger = setup_logger("parser", cfg["general"]["parser_log_file"])
    
    input_dir = cfg["general"]["html_output_dir"]
    output_csv = cfg["general"]["csv_output_file"]
    csv_fields = cfg["general"]["csv_fields"]
    
    # 기존 CSV 파일이 있다면 삭제하고 새로 시작
    if os.path.exists(output_csv):
        os.remove(output_csv)
        logger.info(f"Removed existing CSV file: {output_csv}")

    logger.info("=== HTML Parser Started ===")

    # config의 endpoints 순서와 매칭하여 카테고리 ID를 찾음
    endpoint_map = {ep.strip("/").split("/")[0]: ep for ep in cfg["site"]["endpoints"]}
    
    # HTML 저장 디렉토리 순회
    for category_name in sorted(os.listdir(input_dir)):
        category_dir = os.path.join(input_dir, category_name)
        if not os.path.isdir(category_dir):
            continue

        logger.info(f"--- Parsing category: {category_name} ---")
        
        # 카테고리 정보 추출
        endpoint = endpoint_map.get(category_name, "")
        segs = [s for s in endpoint.strip("/").split("/") if s]
        category_id = segs[1] if len(segs) >= 2 else None

        # 해당 카테고리의 모든 html 파일 처리
        for filename in sorted(os.listdir(category_dir), key=lambda x: int(re.search(r'\d+', x).group())):
            if not filename.endswith(".html"):
                continue

            file_path = os.path.join(category_dir, filename)
            logger.info(f"[PARSING] {file_path}")
            
            with open(file_path, "r", encoding="utf-8") as f:
                html_content = f.read()

            items = parse_html_file(html_content, cfg, logger)
            if not items:
                logger.info(f"[EMPTY] No items found in {file_path}.")
                continue

            # 각 아이템에 카테고리 정보 추가
            for item in items:
                item.category = category_name
                item.category_id = category_id
            
            write_to_csv(items, output_csv, csv_fields)
            logger.info(f"[SAVED] Parsed {len(items)} items from {file_path}")

    logger.info(f"=== HTML Parser Finished. Data saved to {output_csv} ===")

if __name__ == "__main__":
    run_parser()