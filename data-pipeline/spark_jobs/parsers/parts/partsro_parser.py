# ==============================================================================
# etl_pipelines/parts/parsers/partsro_parser.py
#
# 파츠로 부품 HTML을 파싱하는 Spark Job 스크립트
# - 입력: S3에 저장된 카테고리별 HTML 파일 (wholeTextFiles)
# - 처리:
#   1. 각 상품 카드(card)의 전체 텍스트를 추출합니다.
#   2. 복잡한 정규식을 사용하여 텍스트에서 부품명, 부품코드, 가격을 분리합니다.
#   3. S3 파일 경로에서 카테고리 메타데이터를 추출하여 결합합니다.
# - 출력: S3에 Parquet 형식으로 저장
# ==============================================================================
import argparse
import json
import re
from typing import Dict, List, Optional, Tuple
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType)

# ==============================================================================
# 텍스트 파싱 로직 (매우 특화된 로직)
# ==============================================================================

def _price_to_int(s: Optional[str]) -> Optional[int]:
    """문자열 형태의 가격에서 콤마를 제거하고 정수형으로 변환합니다."""
    if not s: return None
    try:
        return int(s.replace(",", ""))
    except (ValueError, TypeError):
        return None

def _normalize_spaces(s: str) -> str:
    """문자열 내의 연속된 공백을 하나로 만들고 양쪽 끝 공백을 제거합니다."""
    return re.sub(r"\s+", " ", s).strip()

def _remove_token_once(s: str, token: str) -> str:
    """문자열에서 주어진 토큰(부품코드 등)을 한 번만 제거합니다."""
    # 단어 경계를 고려한 정규식 패턴 생성
    pat = re.compile(rf"\b{re.escape(token)}\b")
    if pat.search(s):
        return pat.sub("", s, count=1)
    return s.replace(token, "", 1) # 폴백

def parse_text_to_fields(text: str, rgx: Dict[str, re.Pattern]) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    """
    하나의 텍스트 덩어리에서 이름, 코드, 가격을 추출하는 핵심 로직입니다.
    """
    if not text: return None, None, None
    raw = text.replace("\n", " ").strip()
    if rgx["price_only"].match(raw): return None, None, None

    # 1. 가격 추출
    price_val: Optional[int] = None
    m_price = rgx["price"].search(raw)
    if m_price: price_val = _price_to_int(m_price.group(1))

    # 2. 부품 코드 추출 (괄호 안 -> 영문/숫자 조합 -> 긴 숫자 순으로 탐색)
    code: Optional[str] = None
    m_code_paren = rgx["code_in_paren"].search(raw)
    if m_code_paren:
        code = m_code_paren.group(1)
    else:
        candidates = rgx["code_alnum_with_letter"].findall(raw) + rgx["code_numeric_long"].findall(raw)
        price_str = str(price_val) if price_val is not None else None
        for tok in candidates:
            # 코드가 가격과 동일한 문자열인 경우 제외
            if price_str and tok.replace(",", "") == price_str: continue
            code = tok
            break

    # 3. 이름 정리 (원본 텍스트에서 가격과 코드를 제거)
    name_clean = raw
    if m_price: name_clean = name_clean.replace(m_price.group(0), "")
    if m_code_paren: name_clean = name_clean.replace(m_code_paren.group(0), "")
    elif code: name_clean = _remove_token_once(name_clean, code)
    
    name_clean = _normalize_spaces(name_clean.replace("·", " "))
    name_clean = re.sub(r"\s*[-/]\s*$", "", name_clean) # 끝에 남는 하이픈 제거

    # 필수 필드(이름, 코드, 가격)가 모두 없으면 유효하지 않은 데이터로 간주
    if not name_clean or not code or price_val is None:
        return None, None, None

    return name_clean, code, price_val

def parse_html_to_records(html_content: str, category: str, config: Dict) -> List[Dict]:
    """단일 HTML 내용을 파싱하여 레코드 딕셔너리 리스트를 반환합니다."""
    soup = BeautifulSoup(html_content, "lxml")
    card_selector = config["selectors"]["card"]
    
    # 성능을 위해 정규식을 한 번만 컴파일하여 재사용
    rgx = {key: re.compile(val) for key, val in config["regex"].items()}
    records = []

    for card in soup.select(card_selector):
        text = card.get_text(separator=" ", strip=True)
        name, code, price = parse_text_to_fields(text, rgx)
        
        if name and code and price is not None:
            records.append({"category": category, "name": name, "product_code": code, "price": price})
    return records

# ==============================================================================
# Spark Job 메인 실행 함수
# ==============================================================================

def run_spark_job(source_path: str, dest_path: str, parser_config_json: str):
    spark = SparkSession.builder.appName("Partsro_HTML_Parser").getOrCreate()
    parser_config = json.loads(parser_config_json)

    html_rdd = spark.sparkContext.wholeTextFiles(source_path)

    def process_file(file_tuple: tuple) -> list:
        file_path, html_content = file_tuple
        try:
            # S3 경로에서 카테고리 이름 추출 (예: .../partsro/엔진/page.html -> 엔진)
            category = file_path.split('/')[-2]
        except IndexError:
            category = "unknown"
        return parse_html_to_records(html_content, category, parser_config)

    parsed_rdd = html_rdd.flatMap(process_file)

    if parsed_rdd.isEmpty():
        print("파싱된 데이터가 없습니다. 작업을 종료합니다.")
        spark.stop()
        return

    # DataFrame 스키마 정의
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("price", IntegerType(), True),
    ])

    df = spark.createDataFrame(parsed_rdd, schema=schema)
    print(f"총 {df.count()}개의 아이템이 파싱되었습니다.")
    df.show(10, truncate=False)

    df.write.mode("overwrite").parquet(dest_path)
    print(f"데이터를 {dest_path}에 성공적으로 저장했습니다.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", required=True, help="S3의 원본 HTML 파일 경로")
    parser.add_argument("--dest-path", required=True, help="S3에 저장할 Parquet 파일 경로")
    parser.add_argument("--parser-config-json", required=True, help="파싱 설정이 담긴 JSON 문자열")
    args = parser.parse_args()
    
    run_spark_job(args.source_path, args.dest_path, args.parser_config_json)