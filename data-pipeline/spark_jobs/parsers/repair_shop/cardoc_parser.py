# /opt/bitnami/spark/work/parsers/repair_shop/cardoc_parser.py
# -*- coding: utf-8 -*-
# 실행 예)
# spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
#   /opt/bitnami/spark/work/parsers/repair_shop/cardoc_parser.py \
#   --input-path s3a://team2-html-raw/repair_shop/html/20250823/*.html \
#   --output-path s3a://team2-html-out/repair_shop/parquet/20250823 \
#   --format parquet
import os  # ← 추가

import argparse
import json
import re

from pyspark.sql import SparkSession
from bs4 import BeautifulSoup

# ---------- 파싱 유틸 ----------
KOREAN_ADDR_RE = re.compile(
    r'(?:서울|부산|대구|인천|광주|대전|울산|세종|제주|경기|강원|충북|충남|전북|전남|경북|경남)\s+[^\n]+'
)
DATE_RE = re.compile(r'\b(20\d{2}\.\d{2}\.\d{2})\b')
RATING_5_RE = re.compile(r'(\d+(?:\.\d+)?)\s*/\s*5')
INT_RE = re.compile(r'\b\d{1,4}\b')
UUID_RE = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.I)

def address_from_filename(path: str) -> str:
    """
    파일명 패턴: {shop_id}__{addr_slug}.html  또는  {shop_id}.html
    - addr_slug의 '_'를 공백으로 복원해서 반환
    - 주소가 없으면 빈 문자열 반환
    """
    base = os.path.basename(path)
    # 확장자 제거
    name, _ = os.path.splitext(base)

    # "__" 구분자가 있을 때만 주소 존재
    if "__" in name:
        _, addr_slug = name.split("__", 1)
        # 언더스코어만 공백으로 복원 (사용자 요청)
        addr = addr_slug.replace("_", " ").strip()
        return addr
    return ""

def norm(s):
    if not s:
        return None
    return re.sub(r'\s+', ' ', s).strip()

def count_star_svgs(container):
    if not container:
        return None
    yellow = 0
    for svg in container.find_all("svg"):
        try:
            path = svg.find("path")
            fill = (path.get("fill") if path else None) or svg.get("fill") or ""
            if "#FFCA35" in str(fill).upper():
                yellow += 1
        except Exception:
            continue
    return yellow or None

def parse_tags(aspects_row):
    # 항상 리스트 반환(일관 타입)
    tags = []
    if not aspects_row:
        return tags
    pairs = aspects_row.select("div.flex.items-center.space-x-1.whitespace-nowrap")
    if pairs:
        for p in pairs:
            spans = p.find_all("span")
            if len(spans) >= 2:
                name = norm(spans[0].get_text(strip=True)) or ""
                value = norm(spans[1].get_text(strip=True)) or ""
                if name and value:
                    tags.append({"name": name, "value": value})
    else:
        spans = aspects_row.find_all("span")
        for i in range(0, len(spans) - 1, 2):
            name = norm(spans[i].get_text(strip=True)) or ""
            value = norm(spans[i+1].get_text(strip=True)) or ""
            if name and value:
                tags.append({"name": name, "value": value})
    return tags

def parse_review_card(card):
    try:
        vehicle_el = card.select_one("span.text-sm.font-bold")
        vehicle_title = norm(vehicle_el.get_text(strip=True)) or ""

        stars_row = card.select_one("div.flex.items-center.space-x-1")
        rating = count_star_svgs(stars_row) or 0
        rating = int(rating)

        m_date = DATE_RE.search(card.get_text(" ", strip=True))
        date = m_date.group(1) if m_date else ""

        meta_row = card.select_one("div.flex.space-x-1.text-sm.text-gray-800.font-medium")
        work_meta_text = norm(meta_row.get_text(" ", strip=True)) if meta_row else None
        work_meta = [s.strip() for s in work_meta_text.split("·")] if work_meta_text else []
        # 일관 타입 보장
        work_meta = [s for s in work_meta if s]

        aspects_row = card.select_one("div.flex.text-sm.text-gray-800.font-medium.flex-wrap")
        tags = parse_tags(aspects_row)  # 항상 list

        content = ""
        for p in card.find_all("p"):
            txt = norm(p.get_text(" ", strip=True))
            if txt and len(txt) > 10 and not any(k in txt for k in ["수리퀄리티", "친절도", "견적 정확도"]):
                content = txt
                break

        # 완전 빈 카드 스킵
        if not vehicle_title and not content and rating == 0 and not work_meta and not tags:
            return None

        return {
            "vehicle_title": vehicle_title,
            "rating": rating,
            "date": date,
            "work_meta": work_meta,   # list[str]
            "tags": tags,             # list[{"name","value"}]
            "content": content or ""
        }
    except Exception:
        return None

def parse_reviews(html_text: str):
    # lxml → html.parser (의존성 제거)
    soup = BeautifulSoup(html_text, "html.parser")
    records = []
    for card in soup.select("div.py-5.space-y-4"):
        rec = parse_review_card(card)
        if rec:
            records.append(rec)

    # 중복 제거: (work_meta, content) 기준으로 rating 높은 것 유지
    by_key = {}
    for r in records:
        key = ((norm(" · ".join(r.get("work_meta", []))) or ""), (norm(r.get("content")) or ""))
        keep = by_key.get(key)
        if keep is None or (keep.get("rating", 0) < r.get("rating", 0)):
            by_key[key] = r
    return list(by_key.values())

def build_korean_json(path: str, html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # 정비소명
    el = soup.select_one("header .text-lg")
    shop_name = norm(el.get_text(strip=True)) if el else None
    if not shop_name and soup.title and soup.title.string:
        shop_name = re.sub(r'^\[카닥\]\s*', '', soup.title.string).strip()
    shop_name = shop_name or ""

    # 위치: 파일명에서 우선 추출 ("_" -> " ")
    location = address_from_filename(path)

    # (폴백) 파일명에 주소가 없으면 HTML 텍스트에서 탐색
    if not location:
        text_lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines() if ln.strip()]
        for ln in text_lines:
            m = KOREAN_ADDR_RE.search(ln)
            if m:
                location = m.group(0).strip()
                break
        if not location:
            for ln in text_lines:
                if any(p in ln for p in ['서울','경기','인천','부산','대구','광주','대전','울산','세종','제주','강원','충북','충남','전북','전남','경북','경남']):
                    if any(tok in ln for tok in ['구','시','군']):
                        location = ln.strip()
                        break

    # 리뷰
    reviews = parse_reviews(html)

    # source_url
    m = UUID_RE.search(path)
    source_url = f"https://repair.cardoc.co.kr/shops/{m.group(1)}" if m else None

    out = {
        "정비소명": shop_name,
        "위치": location or "",
        "리뷰": reviews,
        "source_url": source_url
    }
    return json.dumps(out, ensure_ascii=False)

# ---------- Spark main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-path", required=True, help="S3A 입력 경로(디렉터리/글롭)")
    ap.add_argument("--output-path", required=True, help="S3A 출력 디렉터리(JSON 혹은 Parquet)")
    ap.add_argument("--format", default="parquet", choices=["jsonl","parquet"], help="저장 형식")  # ← 기본 parquet
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("CardocShopParser-KRJSON")
             .getOrCreate())
    sc = spark.sparkContext

    # (path, html) 로 읽기
    rdd = sc.wholeTextFiles(args.input_path)

    # JSON 문자열 RDD 생성
    json_rdd = rdd.map(lambda rec: build_korean_json(rec[0], rec[1]))

    if args.format == "jsonl":
        # 파티션별 JSONL
        json_rdd.saveAsTextFile(args.output_path)
    else:
        # JSON → DF → Parquet (되는 코드와 동일한 경로)
        df = spark.read.json(json_rdd)
        df.write.mode("overwrite").parquet(args.output_path)

    spark.stop()

if __name__ == "__main__":
    main()
