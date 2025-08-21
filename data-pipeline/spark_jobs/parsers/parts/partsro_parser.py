import argparse
import json
import re
from typing import Dict, List, Optional, Tuple

from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType)

# ==============================================================================
# 파싱 로직 (설정 파일 기반)
# ==============================================================================

def _price_to_int(s: Optional[str]) -> Optional[int]:
    if not s:
        return None
    try:
        return int(s.replace(",", ""))
    except (ValueError, TypeError):
        return None

def _normalize_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()

def _remove_token_once(s: str, token: str) -> str:
    pat = re.compile(rf"\b{re.escape(token)}\b")
    if pat.search(s):
        return pat.sub("", s, count=1)
    return s.replace(token, "", 1)

def parse_text_to_fields(text: str, rgx: Dict[str, re.Pattern]) -> Tuple[Optional[str], Optional[str], Optional[int]]:
    if not text:
        return None, None, None

    raw = text.replace("\n", " ").strip()

    if rgx["price_only"].match(raw):
        return None, None, None

    price_val: Optional[int] = None
    m_price = rgx["price"].search(raw)
    if m_price:
        price_val = _price_to_int(m_price.group(1))

    code: Optional[str] = None
    m_code_paren = rgx["code_in_paren"].search(raw)
    if m_code_paren:
        code = m_code_paren.group(1)
    else:
        candidates = []
        candidates.extend(rgx["code_alnum_with_letter"].findall(raw))
        candidates.extend(rgx["code_numeric_long"].findall(raw))
        price_str = str(price_val) if price_val is not None else None
        for tok in candidates:
            if price_str and tok.replace(",", "") == price_str:
                continue
            code = tok
            break

    name_clean = raw
    if m_price:
        name_clean = name_clean.replace(m_price.group(0), "")
    if m_code_paren:
        name_clean = name_clean.replace(m_code_paren.group(0), "")
    elif code:
        name_clean = _remove_token_once(name_clean, code)

    name_clean = _normalize_spaces(name_clean.replace("·", " "))
    name_clean = re.sub(r"\s*[-/]\s*$", "", name_clean)

    if not name_clean or not code or price_val is None:
        return None, None, None

    return name_clean, code, price_val

def parse_html_to_records(html_content: str, category: str, config: Dict) -> List[Dict]:
    soup = BeautifulSoup(html_content, "lxml")
    card_selector = config["selectors"]["card"]
    
    # 호출당 한 번만 정규식 컴파일
    rgx = {key: re.compile(val) for key, val in config["regex"].items()}

    records = []
    for card in soup.select(card_selector):
        text = card.get_text(separator=" ", strip=True)
        name, code, price = parse_text_to_fields(text, rgx)
        
        if name and code and price is not None:
            records.append({
                "category": category,
                "name": name,
                "product_code": code,
                "price": price
            })
    return records

# ==============================================================================
# Spark Job 메인 로직
# ==============================================================================

def run_spark_job(source_path: str, dest_path: str, parser_config_json: str):
    spark = SparkSession.builder.appName("Partsro_HTML_Parser").getOrCreate()
    parser_config = json.loads(parser_config_json)

    html_rdd = spark.sparkContext.wholeTextFiles(source_path)

    def process_file(file_tuple):
        file_path, html_content = file_tuple
        # S3 경로에서 카테고리 이름 추출 (예: s3a://.../parts/partsro/엔진/page.html -> 엔진)
        try:
            category = file_path.split('/')[-2]
        except IndexError:
            category = "unknown"
        return parse_html_to_records(html_content, category, parser_config)

    parsed_rdd = html_rdd.flatMap(process_file)

    if parsed_rdd.isEmpty():
        print("No data was parsed. Exiting job.")
        spark.stop()
        return

    schema = StructType([
        StructField("category", StringType(), True),
        StructField("name", StringType(), True),
        StructField("product_code", StringType(), True),
        StructField("price", IntegerType(), True),
    ])

    df = spark.createDataFrame(parsed_rdd, schema=schema)
    print(f"Total items parsed: {df.count()}")
    df.show(10, truncate=False)

    df.write.mode("overwrite").parquet(dest_path)
    print(f"Successfully saved data to {dest_path}")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", required=True, help="S3 path for raw HTML files")
    parser.add_argument("--dest-path", required=True, help="S3 path for output Parquet files")
    parser.add_argument("--parser-config-json", required=True, help="JSON string of parser configuration")
    args = parser.parse_args()

    run_spark_job(
        source_path=args.source_path,
        dest_path=args.dest_path,
        parser_config_json=args.parser_config_json,
    )
