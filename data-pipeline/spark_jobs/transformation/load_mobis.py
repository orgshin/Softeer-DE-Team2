# ==============================================================================
# etl_pipelines/parts/transformation/load_mobis.py
#
# S3에 저장된 Mobis Parquet 파일을 읽어 PostgreSQL에 적재하는 Spark Job
# - 입력: S3의 'mobis.parquet' 파일
# - 처리:
#   1. 컬럼명을 표준 한글명으로 변경합니다.
#   2. 가격 컬럼('4,180 원')을 정수형(4180)으로 정제합니다.
#   3. 고유 ID를 생성하고 컬럼 순서를 정리합니다.
# - 출력: PostgreSQL 테이블에 데이터 적재 (Append 모드)
# ==============================================================================
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

def main():
    """메인 실행 함수"""
    # --- 1. DAG로부터 인자 파싱 ---
    parser = argparse.ArgumentParser(description="Load Mobis Parquet from S3 to PostgreSQL")
    parser.add_argument("--s3-path", required=True, help="S3 Parquet 파일의 전체 경로 (예: s3a://bucket/prefix/mobis.parquet)")
    parser.add_argument("--pg-host", required=True, help="PostgreSQL 호스트")
    parser.add_argument("--pg-port", default="5432", help="PostgreSQL 포트")
    parser.add_argument("--pg-db", required=True, help="PostgreSQL 데이터베이스")
    parser.add_argument("--pg-table", required=True, help="데이터를 적재할 테이블")
    parser.add_argument("--pg-user", required=True, help="PostgreSQL 사용자")
    parser.add_argument("--pg-password", required=True, help="PostgreSQL 비밀번호")
    parser.add_argument("--pg-sslmode", default="require", help="PostgreSQL SSL 모드")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("MobisParquetToPostgres").getOrCreate()

    # --- 2. S3에서 Parquet 파일 읽기 ---
    print(f"[*] S3로부터 Parquet 파일을 읽는 중: {args.s3_path}")
    try:
        df = spark.read.parquet(args.s3_path)
    except Exception as e:
        if "Path does not exist" in str(e):
            print(f"[!] 오류: S3 경로를 찾을 수 없습니다: {args.s3_path}")
            spark.stop()
            return
        raise e

    # --- 3. 데이터 정제 및 변환 ---
    # 컬럼명 표준화 (한글)
    column_mapping = {
        "제조사": "제조사", "차량구분": "차량구분", "모델": "모델",
        "부품번호": "부품번호", "한글부품명": "한글부품명", "영문부품명": "영문부품명", "가격": "가격"
    }
    renamed_df = df.toDF(*column_mapping.keys()).select(*column_mapping.values())

    # 가격 컬럼 정제: '4,180 원' -> 4180 (정수 타입)
    cleaned_df = renamed_df.withColumn(
        "가격",
        F.regexp_replace(F.col("가격"), r'[^0-9]', '').cast(IntegerType())
    )
    print("[*] 컬럼명 변경 및 가격 정제 완료:")
    cleaned_df.printSchema()

    # 고유 ID 추가 (row_number 사용)
    window_spec = Window.orderBy(F.col("부품번호"))
    df_with_id = cleaned_df.withColumn("id", F.row_number().over(window_spec))

    # 최종 컬럼 순서 정리 (id를 맨 앞으로)
    final_df = df_with_id.select("id", *column_mapping.values())
    print(f"\n[*] 최종 데이터 개수: {final_df.count()}")
    print("[*] 적재될 데이터 샘플:")
    final_df.show(5, truncate=False)

    # --- 4. PostgreSQL에 데이터 적재 ---
    jdbc_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}"
    print(f"\n[*] PostgreSQL 테이블에 데이터 적재 시작: {args.pg_table}")
    
    (final_df.write
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", args.pg_table)
        .option("user", args.pg_user)
        .option("password", args.pg_password)
        .option("driver", "org.postgresql.Driver")
        .option("sslmode", args.pg_sslmode)
        .mode("append") # 데이터 추가
        .save()
    )

    print(f"\n[성공] PostgreSQL 테이블 '{args.pg_table}'에 데이터 적재를 완료했습니다.")
    spark.stop()

if __name__ == "__main__":
    main()