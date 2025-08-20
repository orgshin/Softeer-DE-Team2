import os
import re
import csv
import logging
from typing import Dict, List, Optional

import yaml
import pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, ValidationError
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ==============================================================================
# 데이터 유효성 검증을 위한 Pydantic 모델
# ==============================================================================

class PartItem(BaseModel):
    category: str = None
    name: str
    part_code: str = None
    price: int

# ==============================================================================
# 설정 및 로깅 (공통 모듈로 분리 가능)
# ==============================================================================

def load_config(path: str) -> dict:
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

def parse_html_content(html_content: str, cfg: dict) -> List[PartItem]:
    """단일 HTML 문자열의 내용을 파싱하여 PartItem 리스트를 반환합니다."""
    soup = BeautifulSoup(html_content, "html.parser")
    cfg_sel = cfg["selectors"]
    cfg_parse = cfg["parsing"]
    container = soup.select_one(cfg_sel["list_container"]) or soup
    
    validated_items: List[PartItem] = []
    
    for li in container.select(cfg_sel["item_li"]):
        name_tag = next((li.select_one(sel) for sel in cfg_sel["name_candidates"] if li.select_one(sel)), None)
        if not name_tag:
            continue
        raw_name = name_tag.get_text(strip=True)
        name, part_code = split_name_and_code(raw_name, cfg_parse["part_code_regex"])

        price_tag = extract_visible_price_tag(li, cfg_sel)
        price = parse_price(price_tag.get_text() if price_tag else "", cfg_parse["price_regex"])

        try:
            item = PartItem(name=name, part_code=part_code, price=price)
            validated_items.append(item)
        except ValidationError as e:
            # Airflow에서는 print 대신 logger 사용을 권장
            logging.warning(f"Data validation failed for '{raw_name}'. Errors: {e.errors()}")
            
    return validated_items

def parse_s3_html_to_s3_parquet(config_path: str):
    """
    S3에서 HTML을 읽어 파싱한 후, 결과를 다른 S3 버킷에 Parquet으로 저장합니다.
    Airflow PythonOperator에서 호출될 메인 함수입니다.
    """
    cfg = load_config(config_path)
    logger = logging.getLogger("airflow.task")

    # S3 연결 설정
    s3_cfg = cfg["s3"]
    s3_output_cfg = cfg["s3_output"]
    
    source_hook = S3Hook(aws_conn_id=s3_cfg["aws_conn_id"])
    dest_hook = S3Hook(aws_conn_id=s3_output_cfg["aws_conn_id"])

    source_bucket = s3_cfg["bucket"]
    source_prefix = s3_cfg["prefix"]
    
    dest_bucket = s3_output_cfg["bucket"]
    dest_key = s3_output_cfg["key"]

    logger.info(f"=== S3 HTML Parser Started ===")
    logger.info(f"Source: s3://{source_bucket}/{source_prefix}")
    logger.info(f"Destination: s3://{dest_bucket}/{dest_key}")

    all_parsed_items = []
    
    # 1. 소스 버킷에서 HTML 파일 키 목록 가져오기
    html_keys = source_hook.list_keys(bucket_name=source_bucket, prefix=source_prefix)
    if not html_keys:
        logger.warning("No HTML files found in the source path. Exiting.")
        return

    for key in html_keys:
        if not key.endswith(".html"):
            continue

        logger.info(f"[PARSING] s3://{source_bucket}/{key}")
        
        # 2. S3에서 HTML 파일 내용 읽기
        html_content = source_hook.read_key(key=key, bucket_name=source_bucket)
        
        # 3. HTML 파싱
        items = parse_html_content(html_content, cfg)
        if not items:
            logger.info(f"[EMPTY] No items found in {key}.")
            continue
            
        # 4. 카테고리 정보 추가 (S3 key 경로에서 추출)
        try:
            category_name = key.split('/')[-2] # e.g., 'parts/hyunki/엔진/page_1.html' -> '엔진'
            for item in items:
                item.category = category_name
            all_parsed_items.extend(items)
            logger.info(f"Parsed {len(items)} items from {key}")
        except IndexError:
            logger.warning(f"Could not determine category from key: {key}")

    if not all_parsed_items:
        logger.warning("No items were parsed in total. No output file will be generated.")
        return
        
    # 5. Pandas DataFrame으로 변환
    logger.info(f"Converting {len(all_parsed_items)} items to DataFrame.")
    items_dict = [item.model_dump() for item in all_parsed_items]
    df = pd.DataFrame(items_dict)
    
    # 6. DataFrame을 Parquet 바이트로 변환
    logger.info("Converting DataFrame to Parquet format in memory.")
    parquet_buffer = df.to_parquet(index=False, engine='pyarrow')

    # 7. Parquet 바이트를 S3에 업로드
    dest_hook.load_bytes(
        bytes_data=parquet_buffer,
        key=dest_key,
        bucket_name=dest_bucket,
        replace=True
    )
    
    logger.info(f"=== S3 Parser Finished. Data saved to s3://{dest_bucket}/{dest_key} ===")
