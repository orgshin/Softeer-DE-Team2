import argparse
import json
import re
from typing import Dict, List, Optional

from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType)

# ==============================================================================
# 파싱 로직 (이전과 동일, 변경 없음)
# ==============================================================================
def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()

def split_name_and_code(name: str, code_regex: str) -> tuple[str, Optional[str]]:
    name = clean_text(name)
    match = re.search(code_regex, name)
    if match:
        part_name = clean_text(name[: match.start()])
        part_code = match.group(1).strip()
        return part_name, part_code
    return name, None

def parse_price(text: str, price_regex: str) -> Optional[int]:
    match = re.search(price_regex, text or "")
    return int(match.group(0).replace(",", "")) if match else None

def parse_html_to_dicts(html_content: str, category: str, parser_config: dict) -> List[Dict]:
    """단일 HTML을 파싱하여 딕셔너리 리스트를 반환합니다."""
    cfg_sel = parser_config["selectors"]
    cfg_regex = parser_config["regex"]
    
    soup = BeautifulSoup(html_content, "html.parser")
    container = soup.select_one(cfg_sel["list_container"]) or soup
    
    parsed_items = []
    for li in container.select(cfg_sel["item_li"]):
        name_tag = next((li.select_one(sel) for sel in cfg_sel["name_candidates"] if li.select_one(sel)), None)
        if not name_tag:
            continue
        raw_name = name_tag.get_text(strip=True)
        name, part_code = split_name_and_code(raw_name, cfg_regex["part_code"])
        price_tag = li.select_one(cfg_sel["price_candidates"][0]) # 단순화를 위해 첫번째 선택자만 사용
        price = parse_price(price_tag.get_text() if price_tag else "", cfg_regex["price"])

        if name and price is not None:
            parsed_items.append({"category": category, "name": name, "part_code": part_code, "price": price})
    return parsed_items

# ==============================================================================
# Spark Job 메인 로직 (대폭 수정)
# ==============================================================================

def run_spark_parser(source_path: str, dest_path: str, parser_config_json: str):
    """PySpark Job의 메인 실행 함수입니다."""
    spark = SparkSession.builder.appName("Hyunki_HTML_Parser").getOrCreate()

    # 인자로 받은 JSON 문자열을 파싱하여 Python 딕셔너리로 변환
    parser_config = json.loads(parser_config_json)

    print(f"Source Path: {source_path}")
    print(f"Destination Path: {dest_path}")

    html_rdd = spark.sparkContext.wholeTextFiles(source_path)

    parsed_rdd = html_rdd.flatMap(lambda file_tuple: 
        parse_html_to_dicts(
            html_content=file_tuple[1],
            category=file_tuple[0].split('/')[-2],
            parser_config=parser_config
        )
    )

    if parsed_rdd.isEmpty():
        print("파싱된 데이터가 없습니다. 작업을 종료합니다.")
        spark.stop()
        return

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
    print(f"데이터를 성공적으로 {dest_path}에 저장했습니다.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", type=str, required=True, help="S3의 원본 HTML 경로")
    parser.add_argument("--dest-path", type=str, required=True, help="S3에 저장할 Parquet 경로")
    parser.add_argument("--parser-config-json", type=str, required=True, help="파싱 규칙이 담긴 JSON 문자열")
    args = parser.parse_args()
    
    run_spark_parser(
        source_path=args.source_path,
        dest_path=args.dest_path,
        parser_config_json=args.parser_config_json,
    )
