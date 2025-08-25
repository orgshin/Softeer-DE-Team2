# ==============================================================================
# etl_pipelines/parts/parsers/mobis_parser.py
#
# 현대모비스 부품 HTML을 파싱하는 Spark Job 스크립트
# - 입력: S3에 저장된 HTML 파일 (wholeTextFiles)
# - 처리:
#   1. S3 파일 경로에서 제조사, 차량구분, 모델 등 메타데이터를 추출합니다.
#      (경로 구조: s3a://.../parts/mobis_parts/{제조사}/{차량구분}/{모델}/{파일명}.html)
#   2. BeautifulSoup를 사용하여 HTML 내의 부품 테이블을 파싱합니다.
#   3. 추출된 메타데이터와 파싱된 부품 정보를 결합합니다.
# - 출력: S3에 Parquet 형식으로 저장
# ==============================================================================
import argparse
import json
import re
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

# ==============================================================================
# 파싱 헬퍼 함수
# ==============================================================================

def clean_text(text: str) -> str:
    """문자열의 불필요한 공백을 정리합니다."""
    return re.sub(r"\s+", " ", text.strip()) if text else ""

def parse_html_content(content: str, selectors: dict) -> list:
    """단일 HTML 내용을 파싱하여 부품 정보 딕셔너리 리스트를 반환합니다."""
    soup = BeautifulSoup(content, "lxml")
    records = []
    
    table_wrap = soup.select_one(selectors["table_wrap"])
    if not table_wrap:
        return records

    for row_element in table_wrap.select(selectors["row"]):
        cells = row_element.select(selectors["cell"])
        if len(cells) < 4:
            continue
        
        part_no = cells[0].get_text(strip=True)
        if part_no:
            records.append({
                "부품번호": clean_text(part_no),
                "한글부품명": clean_text(cells[1].get_text(strip=True)),
                "영문부품명": clean_text(cells[2].get_text(strip=True)),
                "가격": clean_text(cells[3].get_text(strip=True)),
            })
    return records

# ==============================================================================
# Spark Job 메인 실행 함수
# ==============================================================================

def run_spark_job(source_path: str, dest_path: str, parser_config: dict):
    """Spark Job을 실행하는 메인 함수입니다."""
    spark = SparkSession.builder.appName("Mobis_HTML_Parser").getOrCreate()
    
    # S3 경로의 모든 HTML 파일을 (파일경로, 파일내용) RDD로 읽어옵니다.
    html_rdd = spark.sparkContext.wholeTextFiles(source_path)

    def process_file(file_tuple: tuple) -> list:
        """단일 파일을 처리하여 메타데이터와 파싱 결과를 결합합니다."""
        file_path, content = file_tuple
        
        # 파일 경로를 분석하여 메타데이터(제조사, 차종, 모델) 추출
        try:
            _, maker, vtype, model, _ = file_path.split("/")[-5:]
        except ValueError:
            maker, vtype, model = ("unknown", "unknown", "unknown")
        
        # HTML 파싱 실행
        parsed_rows = parse_html_content(content, parser_config["selectors"])
        
        # 각 레코드에 메타데이터 추가
        for row in parsed_rows:
            row.update({"제조사": maker, "차량구분": vtype, "모델": model.replace("_", " ")})
        return parsed_rows

    # FlatMap을 사용하여 모든 파일의 파싱 결과를 단일 RDD로 통합
    parsed_rdd = html_rdd.flatMap(process_file)
    
    if parsed_rdd.isEmpty():
        print(f"경고: {source_path} 경로에서 파싱할 데이터를 찾을 수 없습니다. 작업을 종료합니다.")
        spark.stop()
        return

    # RDD로부터 DataFrame 생성 및 S3에 Parquet으로 저장
    df = spark.createDataFrame(parsed_rdd)
    df.write.mode("overwrite").parquet(dest_path)
    
    print(f"총 {df.count()}개의 레코드를 {dest_path} 경로에 성공적으로 저장했습니다.")
    spark.stop()

if __name__ == "__main__":
    # DAG에서 --application-args로 전달받을 인자 파서 설정
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-path", required=True, help="S3의 원본 HTML 파일 경로")
    parser.add_argument("--dest-path", required=True, help="S3에 저장할 Parquet 파일 경로")
    parser.add_argument("--parser-config-json", required=True, help="파싱 설정이 담긴 JSON 문자열")
    args = parser.parse_args()
    
    # JSON 문자열 설정을 Python 딕셔너리로 변환
    parser_config_data = json.loads(args.parser_config_json)
    
    # Spark Job 실행
    run_spark_job(args.source_path, args.dest_path, parser_config_data)