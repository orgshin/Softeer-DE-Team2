# /opt/bitnami/spark/work/parsers/parts/mobis_parser.py
import argparse
import json
import re
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

def clean(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip()) if text else ""

def parse_html_content(content: str, selectors: dict):
    """
    HTML 내용과 셀렉터 설정을 받아 부품 데이터 리스트를 반환합니다.
    """
    soup = BeautifulSoup(content, "lxml")
    rows = []
    table_wrap = soup.select_one(selectors["table_wrap"])
    if not table_wrap: return rows

    for li in table_wrap.select(selectors["row"]):
        vals = li.select(selectors["cell"])
        if len(vals) < 4: continue
        part_no = vals[0].get_text(strip=True)
        if part_no:
            rows.append({
                "부품번호": clean(part_no), "한글부품명": clean(vals[1].get_text(strip=True)),
                "영문부품명": clean(vals[2].get_text(strip=True)), "가격": clean(vals[3].get_text(strip=True)),
            })
    return rows

def main(source_path: str, dest_path: str, parser_config: dict):
    """
    Spark 잡 메인 함수
    """
    spark = SparkSession.builder.appName("MobisParserFromS3").getOrCreate()
    
    html_rdd = spark.sparkContext.wholeTextFiles(source_path)

    def process_file(file_tuple):
        file_path, content = file_tuple
        try: # 경로에서 메타데이터 추출: mobis_parts/{maker}/{vtype}/{model}/{filename}
            _, maker, vtype, model, _ = file_path.split("/")[-5:]
        except ValueError:
            maker, vtype, model = ("unknown", "unknown", "unknown")
        
        parsed_rows = parse_html_content(content, parser_config["selectors"])
        for row in parsed_rows:
            row.update({"제조사": maker, "차량구분": vtype, "모델": model.replace("_", " ")})
        return parsed_rows

    parsed_rdd = html_rdd.flatMap(process_file)
    if parsed_rdd.isEmpty():
        print(f"경고: {source_path}에서 파싱할 데이터를 찾을 수 없습니다.")
        spark.stop()
        return

    df = spark.createDataFrame(parsed_rdd)
    df.write.mode("overwrite").parquet(dest_path)
    print(f"총 {df.count()}개 데이터를 {dest_path}에 저장 완료.")
    spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--dest-path", required=True)
    parser.add_argument("--parser-config-json", required=True)
    args = parser.parse_args()
    
    parser_config_data = json.loads(args.parser_config_json)
    
    main(args.source_path, args.dest_path, parser_config_data)