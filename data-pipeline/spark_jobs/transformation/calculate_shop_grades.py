# ==============================================================================
# etl_pipelines/repair_shop/transformation/calculate_shop_grades.py
#
# S3의 정비소 리뷰 Parquet과 표준 데이터를 결합하여 최종 등급을 계산하고 PostgreSQL에 저장
# - 입력:
#   1. S3의 정비소 리뷰 Parquet 파일들
#   2. S3의 전국 자동차 정비업체 표준 데이터 CSV 파일
# - 처리:
#   1. HuggingFace BERT 모델을 사용하여 리뷰 텍스트를 분석합니다.
#      - 긍정/부정 리뷰 분류
#      - 리뷰 내용과 키워드 간의 의미적 유사도를 계산하여 카테고리별 통계 생성
#   2. 정제된 정비소명과 주소를 기준으로 표준 데이터와 조인(Join)하여 '공업사등급'을 매칭합니다.
#      - 1차: 정규화된 이름 기준 매칭
#      - 2차: 정규화된 주소 기준 매칭 (1차 실패 시)
#      - 3차: Levenshtein 거리 기반 퍼지(Fuzzy) 매칭 (2차 실패 시)
# - 출력: PostgreSQL의 최종 정비소 등급 분석 테이블 (Overwrite 또는 Append 모드)
# ==============================================================================
import argparse
import os
import re
import json
import logging
from typing import Optional, List, Dict

import numpy as np
import pandas as pd
import torch

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.functions import col, lit, when, broadcast, levenshtein

# ---------------- argparse ----------------
def parse_args():
    p = argparse.ArgumentParser(description="Repair shop review → grade pipeline")
    # S3 입력
    p.add_argument("--s3-bucket", required=True, help="S3 버킷명 (예: team2_html_out)")
    p.add_argument("--reviews-prefix", default="repair_shop/*/*/*.parquet",
                   help="리뷰 Parquet 경로 패턴 (버킷 내부 prefix)")
    p.add_argument("--standards-key", required=True,
                   help="표준 CSV의 S3 key (버킷 내부 경로)")
    p.add_argument("--standards-encoding", default="cp949",
                   choices=["cp949", "utf-8-sig", "utf-8"],
                   help="표준 CSV 인코딩")
    # HF 캐시(선택)
    p.add_argument("--hf-cache", default=None, help="HuggingFace 모델 캐시 경로")
    # PG 출력
    p.add_argument("--pg-host", required=True)
    p.add_argument("--pg-port", type=int, default=5432)
    p.add_argument("--pg-db", required=True)
    p.add_argument("--pg-user", required=True)
    p.add_argument("--pg-password", required=True)
    p.add_argument("--pg-table", default="public.shop_review_with_grade")
    p.add_argument("--pg-sslmode", default="require", choices=["disable", "require", "verify-ca", "verify-full"])
    p.add_argument("--pg-mode", default="overwrite", choices=["overwrite", "append"])
    return p.parse_args()

# ---------------- 설정/상수 ----------------
CATEGORY_MAPPING: Dict[str, List[str]] = {
    "차체 / 외부": ["차체","범퍼","보닛","후드","펜더","도어","트렁크","테일게이트","루프","사이드미러","라이트","헤드라이트","테일라이트","안개등","윈도우","유리","썬루프","몰딩","판금","도색"],
    "하체 / 프레임": ["하체","프레임","서스펜션","쇼크업소버","스프링","암","부싱","볼조인트","스태빌라이저","조향","스티어링","타이로드","브레이크","패드","디스크","캘리퍼","타이어","휠","얼라인먼트","허브","베어링","샤프트","액슬","서브프레임","크로스멤버"],
    "동력계 (엔진/변속기)": ["엔진","흡기","배기","연료펌프","인젝터","점화","플러그","코일","엔진오일","오일필터","라디에이터","워터펌프","서모스탯","변속기","트랜스미션","클러치","디퍼렌셜","터보","벨트"],
    "전장 (전기/전자)": ["전기","전자","배터리","스타터","알터네이터","전조등","와이퍼","센서","ECU","모듈","퓨즈","릴레이","배선","차량제어","전장"],
    "연료 / 배기 시스템": ["연료","연료탱크","연료필터","촉매","머플러","DPF","EGR"],
}
SIMILARITY_THRESHOLD = 0.6
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
REMOVE_WORDS = ["1급","2급","3급","주식회사","유한회사","기아오토큐","오토큐","애니카랜드","블루핸즈","(주)","(유)","쌍용자동차","현대자동차","르노코리아","쉐보레","쌍용","현대","기아","르노","종합","공업사","자동차정비","자동차공업사"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------- HF Models (driver only) ----------------
from transformers import BertTokenizer, BertModel
from transformers import AutoTokenizer, AutoModelForSequenceClassification

CATEGORY_MODEL_NAME = "beomi/kcbert-base"
SENTIMENT_MODEL_NAME = "WhitePeak/bert-base-cased-Korean-sentiment"

import os

def load_models(hf_cache: str | None):
    # 1) 캐시 경로 결정 및 디렉토리 보장
    cache_dir = hf_cache or "/tmp/hf_cache"
    os.makedirs(cache_dir, exist_ok=True)

    # 2) 허깅페이스 캐시 관련 환경변수 모두 세팅
    os.environ["HF_HOME"] = cache_dir
    os.environ["TRANSFORMERS_CACHE"] = cache_dir
    os.environ["HF_HUB_CACHE"] = cache_dir
    os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"

    from transformers import BertTokenizer, BertModel
    from transformers import AutoTokenizer, AutoModelForSequenceClassification

    CATEGORY_MODEL_NAME = "beomi/kcbert-base"
    SENTIMENT_MODEL_NAME = "WhitePeak/bert-base-cased-Korean-sentiment"

    # 3) cache_dir를 명시적으로 전달
    tok = BertTokenizer.from_pretrained(CATEGORY_MODEL_NAME, cache_dir=cache_dir)
    enc_model = BertModel.from_pretrained(CATEGORY_MODEL_NAME, cache_dir=cache_dir).to(DEVICE).eval()

    sent_tok = AutoTokenizer.from_pretrained(SENTIMENT_MODEL_NAME, cache_dir=cache_dir)
    sent_model = AutoModelForSequenceClassification.from_pretrained(
        SENTIMENT_MODEL_NAME, cache_dir=cache_dir
    ).to(DEVICE).eval()

    return tok, enc_model, sent_tok, sent_model


def _mean_pool(last_hidden_state, attention_mask):
    mask = attention_mask.unsqueeze(-1).expand(last_hidden_state.size()).float()
    summed = (last_hidden_state * mask).sum(dim=1)
    denom = mask.sum(dim=1).clamp(min=1e-9)
    return summed / denom

@torch.no_grad()
def embed_text(text: str, tok, enc_model) -> np.ndarray:
    tokens = tok(text, return_tensors="pt", truncation=True, padding=True).to(DEVICE)
    outputs = enc_model(**tokens)
    sent_emb = _mean_pool(outputs.last_hidden_state, tokens["attention_mask"])
    sent_emb = torch.nn.functional.normalize(sent_emb, p=2, dim=-1)
    return sent_emb.cpu().numpy()

@torch.no_grad()
def is_positive_review(text: str, sent_tok, sent_model) -> bool:
    if not text or not text.strip():
        return False
    tokens = sent_tok(text, return_tensors="pt", truncation=True, padding=True).to(DEVICE)
    logits = sent_model(**tokens).logits
    pred = int(torch.argmax(torch.softmax(logits, dim=-1), dim=-1).item())  # 0 neg / 1 pos
    return pred == 1

# ---------------- Normalizers ----------------
def normalize_name_py(name: str) -> str:
    if not isinstance(name, str): return ""
    original = name
    for w in REMOVE_WORDS:
        name = name.replace(w, "")
    name = re.sub(r"[^가-힣a-zA-Z0-9]", "", name)
    return name if name else re.sub(r"[^가-힣a-zA-Z0-9]", "", original)

def normalize_address_py(addr: str) -> Optional[str]:
    if not isinstance(addr, str) or not addr.strip():
        return None
    m = re.search(r"([가-힣0-9]+?(로|길|번길))\s*([0-9-]+)", addr)
    if m:
        return re.sub(r"[-\s]", "", m.group(1) + m.group(3))
    m2 = re.search(r"([가-힣0-9]+?(로|길|번길))", addr)
    if m2:
        return re.sub(r"[-\s]", "", m2.group(1))
    return None

normalize_name_udf = F.udf(normalize_name_py, T.StringType())
normalize_addr_udf = F.udf(normalize_address_py, T.StringType())

def extract_reviews_any_py(val) -> List[str]:
    try:
        if val is None:
            return []
        if isinstance(val, list):
            out = []
            for it in val:
                if isinstance(it, dict):
                    txt = it.get("content")
                    if isinstance(txt, str) and txt.strip():
                        out.append(txt.strip())
                elif isinstance(it, str) and it.strip():
                    out.append(it.strip())
            return out
        if isinstance(val, dict):
            txt = val.get("content")
            return [txt.strip()] if isinstance(txt, str) and txt.strip() else []
        if isinstance(val, str):
            try:
                obj = json.loads(val)
                return extract_reviews_any_py(obj)
            except Exception:
                return [val.strip()] if val.strip() else []
        return []
    except Exception:
        return []

extract_reviews_udf = F.udf(extract_reviews_any_py, T.ArrayType(T.StringType()))

def pick_first_exist(cands: List[str], cols: List[str]) -> Optional[str]:
    for c in cands:
        if c in cols: return c
    return None

# ---------------- Spark ----------------
# /opt/bitnami/spark/work/transformation/repair_shop_review_grade.py

def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("RepairShop-Review-Grade")
        # Airflow에서 이미 s3a.impl 설정을 conf로 넘겨주므로 이 줄도 사실상 중복입니다.
        # 하지만 안전을 위해 남겨두거나 삭제해도 좋습니다.
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

# ---------------- Driver-side NLP ----------------
def analyze_reviews_driver(df_pd: pd.DataFrame, tok, enc_model, sent_tok, sent_model) -> pd.DataFrame:
    # precompute keyword embeddings
    category_embeddings = {}
    for cat, kws in CATEGORY_MAPPING.items():
        if kws:
            embs = [embed_text(k, tok, enc_model) for k in kws]
            category_embeddings[cat] = np.vstack(embs)

    rows = []
    for _, row in df_pd.iterrows():
        shop_name = row["shop_name"]
        address = row["address"]
        reviews: List[str] = row.get("reviews", []) or []

        pos = []
        for r in reviews:
            try:
                if is_positive_review(str(r), sent_tok, sent_model):
                    pos.append(str(r))
            except Exception:
                pass

        pos_cnt = len(pos)
        neg_cnt = max(0, len(reviews) - pos_cnt)

        cat_counts = {k: 0 for k in CATEGORY_MAPPING.keys()}
        for r in pos:
            try:
                r_emb = embed_text(r, tok, enc_model).reshape(-1)
                for cat, kw_mat in category_embeddings.items():
                    if float(np.max(kw_mat @ r_emb)) >= SIMILARITY_THRESHOLD:
                        cat_counts[cat] += 1
            except Exception:
                pass

        out = {
            "shop_name": shop_name,
            "address": address,
            "review_count": len(reviews),
            "positive_reviews": pos_cnt,
            "negative_reviews": neg_cnt,
            "positive_ratio": round(pos_cnt / max(1, len(reviews)), 2),
            "negative_ratio": round(neg_cnt / max(1, len(reviews)), 2),
        }
        out.update(cat_counts)
        rows.append(out)
    return pd.DataFrame(rows)

# ---------------- Main ----------------
def main():
    args = parse_args()
    if args.hf_cache:
        os.environ["TRANSFORMERS_CACHE"] = args.hf_cache

    tok, enc_model, sent_tok, sent_model = load_models(args.hf_cache)
    spark = build_spark()

    # 1) Read S3 Parquet
    parquet_path = f"s3a://{args.s3_bucket}/{args.reviews_prefix}"
    raw = spark.read.parquet(parquet_path)
    cols = raw.columns
    shop_col = pick_first_exist(["shop_name","정비소명","상호"], cols)
    addr_col = pick_first_exist(["address","위치","소재지도로명주소","소재지지번주소","주소"], cols)
    reviews_col = pick_first_exist(["reviews","리뷰","review_list","review"], cols)
    if not shop_col or not addr_col or not reviews_col:
        raise RuntimeError(f"필수 컬럼 미존재. columns={cols}")

    df = raw.select(
        col(shop_col).alias("shop_name"),
        col(addr_col).alias("address"),
        extract_reviews_udf(col(reviews_col)).alias("reviews")
    )

    agg = (
        df.groupBy("shop_name","address")
          .agg(F.flatten(F.collect_list("reviews")).alias("reviews"))
    )

    # 2) NLP 분석 (driver)
    shops_pd = agg.toPandas()
    analyzed_pd = analyze_reviews_driver(shops_pd, tok, enc_model, sent_tok, sent_model)
    left = spark.createDataFrame(analyzed_pd)

    # 3) 표준 CSV
    standards_path = f"s3a://{args.s3_bucket}/{args.standards_key}"
    standards = (
        spark.read.format("csv")
        .option("header", True)
        .option("encoding", args.standards_encoding)
        .load(standards_path)
    )

    r_name = pick_first_exist(["자동차정비업체명","정비소명","업체명"], standards.columns)
    r_grade = pick_first_exist(["자동차정비업체종류","공업사등급"], standards.columns)
    r_addr  = pick_first_exist(["소재지도로명주소","소재지지번주소","주소"], standards.columns)
    if not r_name or not r_grade:
        raise RuntimeError("표준 CSV에 업체명/등급 컬럼 없음")

    # 4) 정규화 키
    left = left.withColumn("_norm_name", normalize_name_udf(col("shop_name"))) \
               .withColumn("_norm_addr", normalize_addr_udf(col("address")))

    right_name = standards.withColumn("_norm_name", normalize_name_udf(col(r_name))) \
                          .select("_norm_name", col(r_grade).alias("grade_name")) \
                          .dropna().dropDuplicates(["_norm_name"])

    # 5) 1차 이름 매칭
    merged = left.join(broadcast(right_name), on="_norm_name", how="left")

    # 6) 2차 주소 매칭 (이름 미매칭만)
    if r_addr:
        right_addr = standards.select(
            normalize_addr_udf(col(r_addr)).alias("_norm_addr"),
            col(r_grade).alias("grade_addr")
        ).dropna().dropDuplicates(["_norm_addr"])
        unmatched = merged.filter(col("grade_name").isNull())
        rematched = unmatched.join(broadcast(right_addr), on="_norm_addr", how="left") \
                             .select(col("_norm_name"), col("_norm_addr"),
                                     *[c for c in unmatched.columns if c not in ("grade_name","grade_addr")],
                                     col("grade_addr"))
        merged = merged.join(rematched.select("_norm_name","_norm_addr","grade_addr"),
                             on=["_norm_name","_norm_addr"], how="left") \
                       .withColumn("grade_raw", when(col("grade_name").isNotNull(), col("grade_name"))
                                               .otherwise(col("grade_addr"))) \
                       .drop("grade_name","grade_addr")
    else:
        merged = merged.withColumn("grade_raw", col("grade_name")).drop("grade_name")

    # 7) 3차 퍼지(Levenshtein <= 1)
    std_name_key = standards.select(
        normalize_name_udf(col(r_name)).alias("_norm_name_std"),
        col(r_grade).alias("grade_std")
    ).dropDuplicates(["_norm_name_std"])

    still = merged.filter(col("grade_raw").isNull()) \
                  .select("shop_name","address","_norm_name","_norm_addr")

    fuzzy = still.join(broadcast(std_name_key),
                       on=levenshtein(col("_norm_name"), col("_norm_name_std")) <= 1, how="left") \
                 .withColumn("grade_fuzzy", col("grade_std")) \
                 .select("shop_name","address","_norm_name","_norm_addr","grade_fuzzy")

    final = merged.join(fuzzy, on=["shop_name","address","_norm_name","_norm_addr"], how="left") \
                  .withColumn("grade_final", when(col("grade_raw").isNotNull(), col("grade_raw"))
                                            .otherwise(col("grade_fuzzy")))

    # 8) 등급 컬럼 확정
    def to_int_or_none(x):
        if x is None: return None
        s = str(x).strip()
        if s == "": return None
        try:
            return int(float(s))
        except Exception:
            return None
    to_grade = F.udf(to_int_or_none, T.IntegerType())

    out = final.withColumn("공업사등급", to_grade(col("grade_final")))
    out = out.withColumn("공업사등급",
                         when(col("공업사등급").isNull(), lit("정보 없음")).otherwise(col("공업사등급")))

    want_cols = [
        "shop_name","address","review_count",
        "positive_reviews","negative_reviews","positive_ratio","negative_ratio",
        *CATEGORY_MAPPING.keys(),
        "공업사등급"
    ]
    existing = [c for c in want_cols if c in out.columns]
    missing  = [c for c in want_cols if c not in out.columns]
    result = out.select(*existing, *[lit(0).alias(c) for c in missing])

    # 9) Postgres 적재
    pg_url = f"jdbc:postgresql://{args.pg_host}:{args.pg_port}/{args.pg_db}?sslmode={args.pg_sslmode}"
    (result.write
           .mode(args.pg_mode)
           .format("jdbc")
           .option("url", pg_url)
           .option("dbtable", args.pg_table)
           .option("user", args.pg_user)
           .option("password", args.pg_password)
           .option("driver", "org.postgresql.Driver")
           .save())

    logging.info("✅ Loaded to PostgreSQL: %s", args.pg_table)

if __name__ == "__main__":
    main()
