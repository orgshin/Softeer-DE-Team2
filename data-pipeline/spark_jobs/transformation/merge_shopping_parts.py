#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
S3의 'parts/' 경로 하위 모든 소스의 Parquet 파일들을 **개별적으로 읽어** 정제한 후,
하나의 PostgreSQL 테이블로 병합/적재합니다.
- 'mobis' 소스는 처리에서 제외합니다.
- 스키마가 다른 Parquet 파일들을 안정적으로 처리하기 위해 소스별로 순회합니다.
"""

import argparse
from typing import List
from functools import reduce

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --- 컬럼명 별칭 정의 ---
CATEGORY_ALIASES = ["category_raw", "category", "카테고리"]
NAME_ALIASES     = ["name_raw", "name", "title", "상품명", "부품명"]
PRICE_ALIASES    = ["price", "가격", "판매가", "sale_price", "amount"]
CODE_ALIASES     = ["vendor_code", "product_code", "part_code", "code", "item_code", "부품코드"]

def pick_first_col(df: DataFrame, aliases: List[str]) -> str | None:
    """DataFrame 컬럼과 별칭 리스트를 비교하여 일치하는 첫 번째 컬럼명을 반환합니다."""
    lower_columns = {c.lower(): c for c in df.columns}
    for alias in (a.lower() for a in aliases):
        if alias in lower_columns:
            return lower_columns[alias]
    return None

def build_projection(df: DataFrame, source_name: str) -> DataFrame:
    """DataFrame을 표준화된 5개 컬럼으로 정제하고 변환합니다."""
    cat_col   = pick_first_col(df, CATEGORY_ALIASES)
    name_col  = pick_first_col(df, NAME_ALIASES)
    price_col = pick_first_col(df, PRICE_ALIASES)
    code_col  = pick_first_col(df, CODE_ALIASES)

    def col_or_null(cname: str | None, typ=T.StringType()):
        return F.col(cname) if cname and (cname in df.columns) else F.lit(None).cast(typ)

    price_extracted = F.regexp_replace(col_or_null(price_col).cast("string"), r'[^0-9]', '')
    price_final = F.when((price_extracted.isNull()) | (price_extracted == ""), None).otherwise(price_extracted.cast("double"))

    if name_col:
        name_src = col_or_null(name_col).cast("string")
        code_src = col_or_null(code_col).cast("string")

        code_from_col = F.upper(F.regexp_extract(code_src, r'[A-Za-z0-9-]+', 0))
        code_from_col = F.when(code_from_col != "", code_from_col)

        code_in_paren = F.regexp_extract(name_src, r'\(([A-Z0-9-]{5,})\)', 1)
        code_alnum    = F.regexp_extract(name_src, r'\b(?=[A-Z0-9-]{5,}\b)(?=.*[A-Z])[A-Z0-9-]+\b', 0)
        code_numeric  = F.regexp_extract(name_src, r'\b\d{8,}\b', 0)

        final_code = F.coalesce(
            code_from_col,
            F.when(code_in_paren != "", code_in_paren),
            F.when(code_alnum    != "", code_alnum),
            F.when(code_numeric  != "", code_numeric),
        )
        
        name_clean = name_src
        name_clean = F.regexp_replace(name_clean, r'\s*\([^()]*\)', ' ')
        name_clean = F.regexp_replace(name_clean, r'할인기간.*', '')
        name_clean = F.regexp_replace(name_clean, r'\s*[\d,]+\s*원', '')
        name_clean = F.regexp_replace(name_clean, r'[–—·]', ' ')
        name_clean = F.regexp_replace(name_clean, r'\s+', ' ')
        name_clean = F.trim(name_clean)
        
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-url", required=True, help="S3 입력 기본 경로 (예: s3a://bucket/parts)")
    ap.add_argument("--pg-host", required=True)
    ap.add_argument("--pg-port", default="5432")
    ap.add_argument("--pg-db", required=True)
    ap.add_argument("--pg-table", required=True)
    ap.add_argument("--pg-user", required=True)
    ap.add_argument("--pg-password", required=True)
    ap.add_argument("--pg-sslmode", default="require")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("parts-merge-all-s3-to-postgres-v2").getOrCreate()
    sc = spark.sparkContext
    
    # Hadoop FileSystem API를 사용하여 S3 디렉토리 목록을 가져옵니다.
    URI = sc._gateway.jvm.java.net.URI
    Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    
    base_path = args.s3_url.rstrip("/")
    fs = FileSystem.get(URI(base_path), sc._jsc.hadoopConfiguration())
    
    source_paths = fs.listStatus(Path(base_path))
    
    processed_dfs = []

    for status in source_paths:
        source_path = status.getPath().toString()
        source_name = status.getPath().getName()

        if not status.isDirectory() or source_name == "mobis":
            print(f"[*] Skipping non-directory or 'mobis' source: {source_name}")
            continue

        print(f"\nProcessing source: {source_name}")
        try:
            df_source = spark.read.parquet(source_path)
            
            print(f"  - Found {df_source.count()} rows. Schema:")
            df_source.printSchema()

            df_processed = build_projection(df_source, source_name)
            processed_dfs.append(df_processed)

        except Exception as e:
            if "Path does not exist" in str(e):
                print(f"  - No parquet files found in {source_path}, skipping.")
            else:
                print(f"  - ERROR processing {source_name}: {e}")

    if not processed_dfs:
        print("\n[!] No data processed. Halting execution.")
        spark.stop()
        return

    # 모든 처리된 데이터프레임을 하나로 합칩니다.
    final_df = reduce(DataFrame.unionAll, processed_dfs)
    
    # ✨ [수정] name_raw와 vendor_code가 **모두** NULL인 행만 삭제합니다.
    final_df = final_df.na.drop(subset=["name_raw", "vendor_code"], how="all")
    final_df = final_df.dropDuplicates(["source", "category_raw", "name_raw", "vendor_code", "price"])

    print(f"\n[*] Final total count after processing all sources: {final_df.count()}")
    print("[*] Sample of final data to be written:")
    final_df.show(20, truncate=False)

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

    print(f"\n[SAVE] Successfully wrote to PostgreSQL table '{args.pg_table}'.")
    spark.stop()

if __name__ == "__main__":
    main()
