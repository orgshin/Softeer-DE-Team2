# ==============================================================================
# etl_pipelines/parts/transformation/merge_parts.py
#
# S3 'parts/' 경로 하위의 모든 소스 Parquet 파일들을 통합하여 PostgreSQL에 적재
# - 입력: S3 'parts/' 폴더 내의 각 소스별 디렉토리 (예: hyunki/, partsro/)
# - 처리:
#   1. 'mobis' 소스는 처리에서 제외합니다.
#   2. 각 소스 디렉토리의 Parquet을 순회하며 읽어들입니다.
#   3. 서로 다른 스키마를 가진 Parquet들을 표준 스키마로 정제/통합합니다.
#      (표준 스키마: source, category_raw, name_raw, vendor_code, price)
#   4. 이름(name_raw)과 코드(vendor_code)가 모두 없는 행은 삭제합니다.
# - 출력: PostgreSQL 테이블에 데이터 적재 (Append 모드)
# ==============================================================================
import argparse
from typing import List
from functools import reduce
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --- 컬럼명 표준화를 위한 별칭(Alias) 목록 ---
CATEGORY_ALIASES = ["category_raw", "category", "카테고리"]
NAME_ALIASES     = ["name_raw", "name", "title", "상품명", "부품명", "한글부품명"]
PRICE_ALIASES    = ["price", "가격", "판매가", "sale_price", "amount"]
CODE_ALIASES     = ["vendor_code", "product_code", "part_code", "code", "item_code", "부품번호"]

def pick_first_col(df: DataFrame, aliases: List[str]) -> str | None:
    """DataFrame의 컬럼과 별칭 리스트를 비교하여 일치하는 첫 번째 컬럼명을 반환합니다."""
    lower_columns = {c.lower(): c for c in df.columns}
    for alias in (a.lower() for a in aliases):
        if alias in lower_columns:
            return lower_columns[alias]
    return None

def standardize_schema(df: DataFrame, source_name: str) -> DataFrame:
    """DataFrame을 표준화된 5개 컬럼으로 정제하고 변환합니다."""
    cat_col   = pick_first_col(df, CATEGORY_ALIASES)
    name_col  = pick_first_col(df, NAME_ALIASES)
    price_col = pick_first_col(df, PRICE_ALIASES)
    code_col  = pick_first_col(df, CODE_ALIASES)

    def col_or_null(cname: str | None, typ=T.StringType()):
        """컬럼이 존재하면 해당 컬럼을, 없으면 NULL 리터럴을 반환합니다."""
        return F.col(cname) if cname and (cname in df.columns) else F.lit(None).cast(typ)

    # 가격 정제: 숫자 아닌 문자 모두 제거 후 double 타입으로 변환
    price_extracted = F.regexp_replace(col_or_null(price_col).cast("string"), r'[^0-9]', '')
    price_final = F.when((price_extracted.isNull()) | (price_extracted == ""), None).otherwise(price_extracted.cast("double"))

    # 이름 및 코드 정제
    if name_col:
        name_src = col_or_null(name_col).cast("string")
        code_src = col_or_null(code_col).cast("string")
        
        # 1순위: 원래 코드 컬럼이 있다면 사용
        code_from_col = F.upper(F.regexp_extract(code_src, r'[A-Za-z0-9-]+', 0))
        code_from_col = F.when(code_from_col != "", code_from_col)
        
        # 2순위: 이름 컬럼에서 정규식으로 코드 추출
        code_in_paren = F.regexp_extract(name_src, r'\(([A-Z0-9-]{5,})\)', 1)
        code_alnum    = F.regexp_extract(name_src, r'\b(?=[A-Z0-9-]{5,}\b)(?=.*[A-Z])[A-Z0-9-]+\b', 0)
        code_numeric  = F.regexp_extract(name_src, r'\b\d{8,}\b', 0)
        final_code = F.coalesce(code_from_col, F.when(code_in_paren != "", code_in_paren), F.when(code_alnum != "", code_alnum), F.when(code_numeric != "", code_numeric))
        
        # 이름에서 코드, 가격, 불필요한 문자열 제거
        name_clean = F.trim(F.regexp_replace(F.regexp_replace(F.regexp_replace(F.regexp_replace(name_src, r'\s*\([^()]*\)', ' '), r'할인기간.*', ''), r'\s*[\d,]+\s*원', ''), r'[–—·\s]+', ' '))
        name_final = F.when((name_clean.isNull()) | (name_clean == ""), name_src).otherwise(name_clean)
    else:
        name_final = F.lit(None).cast("string")
        final_code = F.upper(F.regexp_extract(col_or_null(code_col).cast("string"), r'[A-Za-z0-9-]+', 0))

    return df.withColumn("source", F.lit(source_name)) \
             .withColumn("category_raw", col_or_null(cat_col)) \
             .withColumn("name_raw", name_final) \
             .withColumn("vendor_code", final_code) \
             .withColumn("price", price_final) \
             .select("source", "category_raw", "name_raw", "vendor_code", "price")

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="Merge all parts sources from S3 to PostgreSQL")
    parser.add_argument("--s3-url", required=True, help="S3 입력 기본 경로 (예: s3a://bucket/parts)")
    parser.add_argument("--pg-host", required=True)
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-db", required=True)
    parser.add_argument("--pg-table", required=True)
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-password", required=True)
    parser.add_argument("--pg-sslmode", default="require")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("PartsMergeAllS3ToPostgres").getOrCreate()
    sc = spark.sparkContext
    
    # Hadoop FileSystem API를 사용하여 S3의 하위 디렉토리 목록을 가져옵니다.
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    
    base_path = args.s3_url.rstrip("/")
    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())
    source_paths = fs.listStatus(Path(base_path))
    
    processed_dfs = []

    for status in source_paths:
        source_path_str = status.getPath().toString()
        source_name = status.getPath().getName()

        if not status.isDirectory() or source_name == "mobis":
            print(f"[*] 건너뛰기 (디렉토리가 아니거나 'mobis'): {source_name}")
            continue

        print(f"\n처리 시작: {source_name}")
        try:
            df_source = spark.read.parquet(source_path_str)
            print(f"  - {df_source.count()}개 행 발견. 스키마:")
            df_source.printSchema()

            df_processed = standardize_schema(df_source, source_name)
            processed_dfs.append(df_processed)
        except Exception as e:
            if "Path does not exist" in str(e):
                print(f"  - Parquet 파일이 없어 건너뜁니다: {source_path_str}")
            else:
                print(f"  - 처리 중 오류 발생: {e}")

    if not processed_dfs:
        print("\n[!] 처리된 데이터가 없습니다. 작업을 중단합니다.")
        spark.stop()
        return

    # 모든 소스의 DataFrame을 하나로 통합
    final_df = reduce(DataFrame.unionAll, processed_dfs)
    
    # 이름과 코드가 모두 NULL인 행 제거 및 중복 제거
    final_df = final_df.na.drop(subset=["name_raw", "vendor_code"], how="all")
    final_df = final_df.dropDuplicates(["source", "category_raw", "name_raw", "vendor_code", "price"])

    print(f"\n[*] 최종 데이터 개수 (모든 소스 통합 후): {final_df.count()}")
    print("[*] 적재될 최종 데이터 샘플:")
    final_df.show(20, truncate=False)

    # PostgreSQL에 데이터 적재
    url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    (final_df.write
        .format("jdbc")
        .option("url", url)
        .option("dbtable", args.pg_table)
        .option("user", args.pg_user)
        .option("password", args.pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("sslmode", args.pg_sslmode)   # ← 여기서 옵션으로 전달
        .mode("append")
        .save()
    )

    print(f"\n[성공] PostgreSQL 테이블 '{args.pg_table}'에 데이터 적재를 완료했습니다.")
    spark.stop()

if __name__ == "__main__":
    main()
