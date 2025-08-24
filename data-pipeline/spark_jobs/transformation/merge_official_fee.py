import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from io import BytesIO

def merge_s3_parquet_to_postgres(**kwargs):
    """
    S3의 Parquet 파일들을 읽어 DataFrame으로 합친 후,
    컬럼명 영문 변경 및 ID를 추가하여 PostgreSQL에 적재합니다.
    """
    s3_conn_id = kwargs["s3_conn_id"]
    postgres_conn_id = kwargs["postgres_conn_id"]
    s3_bucket = kwargs["s3_bucket"]
    s3_prefix = kwargs["s3_prefix"]
    pg_table = kwargs["pg_table"]

    print(f"S3 Bucket: {s3_bucket}, Prefix: {s3_prefix}")
    print(f"Target PostgreSQL table: {pg_table}")

    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
    keys = s3_hook.list_keys(bucket_name=s3_bucket, prefix=s3_prefix)
    parquet_files = [key for key in keys if key.endswith(".parquet")]
    
    if not parquet_files:
        print("No parquet files found.")
        return

    print(f"Found {len(parquet_files)} parquet files.")

    df_list = []
    for key in parquet_files:
        file_obj = s3_hook.get_key(key=key, bucket_name=s3_bucket)
        buffer = BytesIO(file_obj.get()['Body'].read())
        df = pd.read_parquet(buffer)
        df_list.append(df)

    merged_df = pd.concat(df_list, ignore_index=True)
    print(f"Total rows merged: {len(merged_df)}")

    # ✨ 1. 한글 컬럼명을 영문으로 변경
    column_mapping = {
        "cate_name": "category_name",
        "공임비_num": "fee_amount",
        "소요시간_min": "time_min",
        "소요시간_max": "time_max",
        "작업종류": "work_type"
    }
    merged_df.rename(columns=column_mapping, inplace=True)
    
    # ✨ 2. 고유 ID 컬럼 추가 (기존 인덱스 활용)
    merged_df.reset_index(inplace=True)
    merged_df.rename(columns={'index': 'id'}, inplace=True)

    # 최종 컬럼 순서 정리 (id를 맨 앞으로)
    cols = ['id'] + [col for col in merged_df.columns if col != 'id']
    final_df = merged_df[cols]

    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = pg_hook.get_sqlalchemy_engine()
    
    print(f"Writing data to table '{pg_table}'...")
    final_df.to_sql(
        name=pg_table,
        con=engine,
        if_exists='replace',
        index=False
    )
    print("Successfully loaded data into PostgreSQL.")
