# ==============================================================================
# etl_pipelines/parts/transformation/aggregate_parts_price.py
#
# DW 테이블(통합 부품 정보)을 읽어 부품별 평균 가격을 계산하고 DM 테이블에 저장
# - 입력: PostgreSQL의 통합 부품 정보 테이블 (예: parts_merged)
# - 처리:
#   1. 카테고리(category_raw), 부품명(name_raw), 부품코드(vendor_code)를 기준으로 그룹화합니다.
#   2. 각 그룹의 평균 가격(avg_price)을 계산합니다.
#   3. 고유 ID를 추가하여 최종 DM 테이블 스키마에 맞춥니다.
# - 출력: PostgreSQL의 부품 가격 집계 테이블 (예: parts_avg_price) (Overwrite 모드)
# ==============================================================================
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="Aggregate parts average price from PostgreSQL")
    parser.add_argument("--pg-host", required=True)
    parser.add_argument("--pg-port", default="5432")
    parser.add_argument("--pg-db", required=True)         # 실제 Postgres DB명
    parser.add_argument("--pg-user", required=True)
    parser.add_argument("--pg-password", required=True)
    parser.add_argument("--read-table", required=True)    # 읽을 테이블명 (스키마 포함 가능: public.table)
    parser.add_argument("--write-table", required=True)   # 쓸 테이블명
    parser.add_argument("--pg-sslmode", default="require")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("PostgresPartsAggregation").getOrCreate()

    # --- 1. PostgreSQL에서 데이터 읽기 ---
    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}?sslmode={args.pg_sslmode}"
    properties = {
        "user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"
    }
    print(f"PostgreSQL 테이블에서 데이터 읽는 중: {args.read_table}")
    parts_df = spark.read.jdbc(url=jdbc_url, table=args.read_table, properties=properties)

    # --- 2. 데이터 집계 ---
    print("데이터 집계 시작...")
    avg_price_df = (
        parts_df.groupBy("category_raw", "name_raw", "vendor_code")
                .agg(F.avg("price").alias("avg_price"))
    )

    # 고유 ID 추가 (monotonically_increasing_id는 연속성을 보장하지 않음)
    aggregated_with_id = avg_price_df.withColumn("id", F.monotonically_increasing_id())
    final_df = aggregated_with_id.select("id", "category_raw", "name_raw", "vendor_code", "avg_price")

    # --- 3. PostgreSQL에 데이터 쓰기 ---
    print(f"집계된 데이터를 PostgreSQL 테이블에 쓰는 중: {args.write_table}")
    (final_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", args.write_table)
        .option("user", args.pg_user)
        .option("password", args.pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true") # Overwrite 시 테이블 데이터를 모두 지움
        .mode("overwrite")
        .save()
    )

    print(f"[성공] 집계된 데이터를 '{args.write_table}' 테이블에 저장했습니다.")
    spark.stop()

if __name__ == "__main__":
    main()