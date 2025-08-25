# ==============================================================================
# etl_pipelines/parts/parsers/hyunki_parser.py
#
# 현키몰 부품 HTML을 파싱하는 Spark Job 스크립트
# - 입력: S3에 저장된 카테고리별/페이지별 HTML 파일 (wholeTextFiles)
# - 처리:
#   1. BeautifulSoup와 CSS Selector를 사용하여 각 부품 아이템을 추출합니다.
#   2. 정규식을 사용하여 부품명과 부품코드를 분리하고, 가격을 정수형으로 변환합니다.
#   3. S3 파일 경로에서 카테고리 메타데이터를 추출하여 결합합니다.
# - 출력: S3에 Parquet 형식으로 저장
# ==============================================================================
import argparse
import json
import re
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField, StructType)

# ==============================================================================
# 텍스트 파싱 헬퍼 함수
# ==============================================================================

def clean_text(text: str) -> str:
    """문자열의 불필요한 공백을 정리합니다."""
    return re.sub(r"\s+", " ", text or "").strip()

def split_name_and_code(name: str, code_regex: str) -> tuple[str, Optional[str]]:
    """부품명 텍스트에서 이름과 코드(괄호 안)를 분리합니다."""
    name = clean_text(name)
    match = re.search(code_regex, name)
    if match:
        part_name = clean_text(name[: match.start()])
        part_code = match.group(1).strip()
        return part_name, part_code
    return name, None

def parse_price(text: str, price_regex: str) -> Optional[int]:
    """가격 텍스트에서 숫자만 추출하여 정수형으로 변환합니다."""
    match = re.search(price_regex, text or "")
    return int(match.group(0).replace(",", "")) if match else None

# ==============================================================================
# 메인 HTML 파싱 함수
# ==============================================================================

def parse_html_to_dicts(html_content: str, category: str, parser_config: dict) -> List[Dict]:
    """단일 HTML 내용을 파싱하여 부품 정보 딕셔너리 리스트를 반환합니다."""
    cfg_sel = parser_config["selectors"]
    cfg_regex = parser_config["regex"]
    soup = BeautifulSoup(html_content, "html.parser")
    
    # 상품 목록 컨테이너를 먼저 찾고, 없으면 전체 HTML에서 탐색
    container = soup.select_one(cfg_sel["list_container"]) or soup
    parsed_items = []

    for item_li in container.select(cfg_sel["item_li"]):
        # 여러 이름 후보군 중 가장 먼저 찾아지는 태그를 사용
        name_tag = next((item_li.select_one(sel) for sel in cfg_sel["name_candidates"] if item_li.select_one(sel)), None)
        if not name_tag: continue

        raw_name = name_tag.get_text(strip=True)
        name, part_code = split_name_and_code(raw_name, cfg_regex["part_code"])
        
        # 여러 가격 후보군 중 가장 먼저 찾아지는 태그를 사용
        price_tag = next((item_li.select_one(sel) for sel in cfg_sel["price_candidates"] if item_li.select_one(sel)), None)
        price = parse_price(price_tag.get_text() if price_tag else "", cfg_regex["price"])

        if name and price is not None:
            parsed_items.append({"category": category, "name": name, "part_code": part_code, "price": price})
    return parsed_items

# ==============================================================================
# Spark Job 메인 실행 함수
# ==============================================================================

def run_spark_parser(source_path: str, dest_path: str, parser_config_json: str):
    """PySpark Job의 메인 실행 함수입니다."""
    spark = SparkSession.builder.appName("Hyunki_HTML_Parser").getOrCreate()
    parser_config = json.loads(parser_config_json)

    print(f"Source Path: {source_path}")
    print(f"Destination Path: {dest_path}")

    html_rdd = spark.sparkContext.wholeTextFiles(source_path)

    def process_file(file_tuple: tuple) -> list:
        html_content = file_tuple[1]
        try:
            # S3 경로에서 카테고리 이름 추출 (예: .../hyunki/엔진/page_1.html -> 엔진)
            category = file_tuple[0].split('/')[-2]
        except IndexError:
            category = "unknown"
        return parse_html_to_dicts(html_content, category, parser_config)

    parsed_rdd = html_rdd.flatMap(process_file)

    if parsed_rdd.isEmpty():
        print("파싱된 데이터가 없습니다. 작업을 종료합니다.")
        spark.stop()
        return

    # DataFrame 스키마 정의
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("part_code", StringType(), True),
        StructField("price", IntegerType(), True),
    ])
    
    df = spark.createDataFrame(parsed_rdd, schema=schema)
    print(f"총 {df.count()}개의 아이템이 파싱되었습니다.")
    df.show(5, truncate=False)

    df.write.mode("overwrite").parquet(dest_path)
    print(f"데이터를 {dest_path}에 성공적으로 저장했습니다.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", type=str, required=True, help="S3의 원본 HTML 경로")
    parser.add_argument("--dest-path", type=str, required=True, help="S3에 저장할 Parquet 경로")
    parser.add_argument("--parser-config-json", type=str, required=True, help="파싱 규칙이 담긴 JSON 문자열")
    args = parser.parse_args()
    
    run_spark_parser(args.source_path, args.dest_path, args.parser_config_json)