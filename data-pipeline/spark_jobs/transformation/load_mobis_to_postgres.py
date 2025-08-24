#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
S3의 'mobis.parquet' 파일을 읽어 컬럼명을 한글로 변경하고,
고유 ID를 추가하며 가격을 숫자 타입으로 정제하여 PostgreSQL 테이블에 적재합니다.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

def main():
    """메인 실행 함수"""
    ap = argparse.ArgumentParser()
    ap.add_argument("--s3-path", required=True, help="S3 Parquet 파일의 전체 경로 (예: s3a://bucket/parts/mobis/mobis.parquet)")
    ap.add_argument("--pg-host", required=True, help="PostgreSQL 호스트")
    ap.add_argument("--pg-port", default="5432", help="PostgreSQL 포트")
    ap.add_argument("--pg-db", required=True, help="PostgreSQL 데이터베이스")
    ap.add_argument("--pg-table", required=True, help="데이터를 적재할 테이블")
    ap.add_argument("--pg-user", required=True, help="PostgreSQL 사용자")
    ap.add_argument("--pg-password", required=True, help="PostgreSQL 비밀번호")
    ap.add_argument("--pg-sslmode", default="require")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("MobisParquetToPostgres").getOrCreate()

    print(f"[*] Reading Parquet file from: {args.s3_path}")
    try:
        df = spark.read.parquet(args.s3_path)
    except Exception as e:
        if "Path does not exist" in str(e):
            print(f"\n[!] ERROR: S3 path does not exist: {args.s3_path}")
            spark.stop()
            return
        raise e

    # --- 컬럼명 한글로 변경 ---
    column_mapping = {
        "제조사": "제조사",
        "차량구분": "차량구분",
        "모델": "모델",
        "검색키워드": "검색키워드",
        "부품번호": "부품번호",
        "한글 부품명": "한글부품명",
        "영문 부품명": "영문부품명",
        "가격(부가세포함)": "가격"
    }
    
    renamed_df = df
    for old_name, new_name in zip(df.columns, column_mapping.values()):
        renamed_df = renamed_df.withColumnRenamed(old_name, new_name)

    # ✨ [수정] 가격 컬럼 정제: '4,180 원' -> 4180 (정수 타입)
    # regexp_replace 함수를 사용하여 숫자(0-9)가 아닌 모든 문자를 제거하고, 정수(Integer) 타입으로 변환합니다.
    cleaned_df = renamed_df.withColumn(
        "가격",
        F.regexp_replace(F.col("가격"), r'[^0-9]', '').cast(IntegerType())
    )

    print("[*] Columns renamed and price cleaned:")
    cleaned_df.printSchema()

    # --- 고유 ID 추가 ---
    window_spec = Window.orderBy(F.col("부품번호"))
    df_with_id = cleaned_df.withColumn("id", F.row_number().over(window_spec))

    # --- 최종 컬럼 순서 정리 (id를 맨 앞으로) ---
    final_df = df_with_id.select("id", *column_mapping.values())

    print(f"\n[*] Final data count: {final_df.count()}")
    print("[*] Sample of data to be written:")
    final_df.show(5, truncate=False)

    # --- PostgreSQL에 데이터 적재 ---
    # jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    # properties = {
    #     "user": args.pg_user,
    #     "password": args.pg_password,
    #     "driver": "org.postgresql.Driver"
    # }

    # print(f"\n[*] Writing data to PostgreSQL table: {args.pg_table}")
    # final_df.write.jdbc(
    #     url=jdbc_url,
    #     table=args.pg_table,
    #     mode="overwrite",
    #     properties=properties,
    #     sslmode=args.pg_sslmode
    # )
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
