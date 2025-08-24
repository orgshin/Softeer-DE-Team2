#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pg-host", required=True)
    ap.add_argument("--pg-port", default="5432")
    ap.add_argument("--pg-db", required=True)         # 실제 Postgres DB명
    ap.add_argument("--pg-user", required=True)
    ap.add_argument("--pg-password", required=True)
    ap.add_argument("--read-table", required=True)    # 읽을 테이블명 (스키마 포함 가능: public.table)
    ap.add_argument("--write-table", required=True)   # 쓸 테이블명
    ap.add_argument("--pg-sslmode", default="require")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("PostgresPartsAggregation").getOrCreate()

    # ✅ SSL 모드 포함한 JDBC URL
    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}?sslmode={args.pg_sslmode}"

    # 공통 연결 속성
    properties = {
        "user": args.pg_user,
        "password": args.pg_password,
        "driver": "org.postgresql.Driver",
    }

    print(f"Reading data from PostgreSQL table: {args.read_table}")
    parts_df = spark.read.jdbc(
        url=jdbc_url,
        table=args.read_table,
        properties=properties
    )

    print("Aggregating data...")
    avg_price_df = (
        parts_df.groupBy("category_raw", "name_raw", "vendor_code")
                .agg(F.avg("price").alias("avg_price"))
    )

    # 주의: monotonically_increasing_id()는 파티션별로 증가하며 연속 보장은 없음.
    aggregated_with_id = avg_price_df.withColumn("id", F.monotonically_increasing_id())

    final_df = aggregated_with_id.select("id", "category_raw", "name_raw", "vendor_code", "avg_price")

    print(f"Writing aggregated data to PostgreSQL table: {args.write_table}")
    (final_df.write
        .format("jdbc")
        .option("url", jdbc_url)                 # ✅ URL에 sslmode 포함
        .option("dbtable", args.write_table)
        .option("user", args.pg_user)
        .option("password", args.pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")              # overwrite 시 TRUNCATE 후 insert (테이블 존재 시)
        .mode("overwrite")
        .save()
    )

    print(f"[SUCCESS] Aggregated data written to table '{args.write_table}'.")
    spark.stop()

if __name__ == "__main__":
    main()
