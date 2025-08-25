# ==============================================================================
# etl_pipelines/repair_shop/parsers/cardoc_parser.py
#
# 카닥 정비소 HTML을 파싱하는 Spark Job 스크립트
# - 입력: S3에 저장된 정비소별 HTML 파일 (wholeTextFiles)
# - 처리:
#   1. 파일명에서 주소 메타데이터를 추출합니다.
#      (파일명 패턴: {shop_id}__{addr_slug}.html)
#   2. BeautifulSoup를 사용하여 정비소명, 위치(폴백), 리뷰 목록을 파싱합니다.
#   3. 파싱된 모든 정보를 하나의 JSON 구조로 만듭니다.
# - 출력: S3에 Parquet (또는 JSONL) 형식으로 저장
# ==============================================================================
import os
import argparse
import json
import re
from pyspark.sql import SparkSession
from bs4 import BeautifulSoup

# ==============================================================================
# 파싱 유틸리티 및 정규식
# ==============================================================================

# 주소, 날짜, UUID 등을 텍스트에서 추출하기 위한 정규식
KOREAN_ADDR_RE = re.compile(
    r'(?:서울|부산|대구|인천|광주|대전|울산|세종|제주|경기|강원|충북|충남|전북|전남|경북|경남)\s+[^\n]+'
)
DATE_RE = re.compile(r'\b(20\d{2}\.[0-9]{2}\.[0-9]{2})\b')
UUID_RE = re.compile(r'([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', re.I)

def address_from_filename(path: str) -> str:
    """
    S3 파일명({shop_id}__{addr_slug}.html)에서 주소 슬러그를 추출하고
    언더스코어 '_'를 공백으로 복원하여 주소 문자열을 반환합니다.
    """
    base = os.path.basename(path)
    name, _ = os.path.splitext(base)
    if "__" in name:
        _, addr_slug = name.split("__", 1)
        return addr_slug.replace("_", " ").strip()
    return ""

def normalize_text(s: str) -> str | None:
    """문자열의 불필요한 공백(줄바꿈 포함)을 정리합니다."""
    if not s:
        return None
    return re.sub(r'\s+', ' ', s).strip()

def count_star_svgs(container) -> int | None:
    """별점 SVG 아이콘의 색상(#FFCA35)을 분석하여 평점을 계산합니다."""
    if not container:
        return None
    yellow_stars = 0
    for svg in container.find_all("svg"):
        try:
            path = svg.find("path")
            fill_color = (path.get("fill") if path else None) or svg.get("fill") or ""
            if "#FFCA35" in str(fill_color).upper():  # 노란색 별 카운트
                yellow_stars += 1
        except Exception:
            continue
    return yellow_stars or None

# ==============================================================================
# 리뷰 파싱 로직
# ==============================================================================

def parse_tags(aspects_row) -> list:
    """리뷰의 세부 평가 항목(태그)을 파싱하여 리스트로 반환합니다."""
    tags = []
    if not aspects_row:
        return tags
        
    # 일반적인 태그 구조 파싱
    pairs = aspects_row.select("div.flex.items-center.space-x-1.whitespace-nowrap")
    if pairs:
        for p in pairs:
            spans = p.find_all("span")
            if len(spans) >= 2:
                name = normalize_text(spans[0].get_text(strip=True)) or ""
                value = normalize_text(spans[1].get_text(strip=True)) or ""
                if name and value:
                    tags.append({"name": name, "value": value})
    else:
        # 다른 구조의 태그를 위한 폴백 로직
        spans = aspects_row.find_all("span")
        for i in range(0, len(spans) - 1, 2):
            name = normalize_text(spans[i].get_text(strip=True)) or ""
            value = normalize_text(spans[i+1].get_text(strip=True)) or ""
            if name and value:
                tags.append({"name": name, "value": value})
    return tags

def parse_review_card(card) -> dict | None:
    """개별 리뷰 카드 HTML을 파싱하여 구조화된 딕셔너리로 반환합니다."""
    try:
        vehicle_el = card.select_one("span.text-sm.font-bold")
        vehicle_title = normalize_text(vehicle_el.get_text(strip=True)) if vehicle_el else ""

        stars_row = card.select_one("div.flex.items-center.space-x-1")
        rating = count_star_svgs(stars_row) or 0

        m_date = DATE_RE.search(card.get_text(" ", strip=True))
        date = m_date.group(1) if m_date else ""

        meta_row = card.select_one("div.flex.space-x-1.text-sm.text-gray-800.font-medium")
        work_meta_text = normalize_text(meta_row.get_text(" ", strip=True)) if meta_row else None
        work_meta = [s.strip() for s in work_meta_text.split("·")] if work_meta_text else []
        work_meta = [s for s in work_meta if s] # 빈 문자열 제거

        aspects_row = card.select_one("div.flex.text-sm.text-gray-800.font-medium.flex-wrap")
        tags = parse_tags(aspects_row)

        content = ""
        # 본문은 특정 키워드가 포함되지 않은 긴 텍스트로 판단
        for p in card.find_all("p"):
            txt = normalize_text(p.get_text(" ", strip=True))
            if txt and len(txt) > 10 and not any(k in txt for k in ["수리퀄리티", "친절도", "견적 정확도"]):
                content = txt
                break

        # 필수 정보가 없는 유령 카드는 None을 반환하여 필터링
        if not vehicle_title and not content and rating == 0 and not work_meta and not tags:
            return None

        return {
            "vehicle_title": vehicle_title,
            "rating": int(rating),
            "date": date,
            "work_meta": work_meta,   # list[str]
            "tags": tags,             # list[{"name","value"}]
            "content": content or ""
        }
    except Exception:
        # 파싱 중 예외 발생 시 해당 카드는 건너뜀
        return None

def parse_reviews(html_text: str) -> list:
    """HTML에서 모든 리뷰를 파싱하고 중복을 제거한 리스트를 반환합니다."""
    soup = BeautifulSoup(html_text, "html.parser")
    records = [rec for card in soup.select("div.py-5.space-y-4") if (rec := parse_review_card(card))]
    
    # (작업내용, 리뷰본문) 기준으로 중복 제거 (평점이 더 높은 것을 유지)
    by_key = {}
    for r in records:
        key = (
            (normalize_text(" · ".join(r.get("work_meta", []))) or ""), 
            (normalize_text(r.get("content")) or "")
        )
        if key not in by_key or by_key[key].get("rating", 0) < r.get("rating", 0):
            by_key[key] = r
    return list(by_key.values())

# ==============================================================================
# 메인 파서 및 Spark Job
# ==============================================================================

def build_shop_json(path: str, html: str) -> str:
    """하나의 HTML 파일로부터 정비소 정보를 담은 최종 JSON 문자열을 생성합니다."""
    soup = BeautifulSoup(html, "html.parser")

    # 1. 정비소명 추출
    el = soup.select_one("header .text-lg")
    shop_name = normalize_text(el.get_text(strip=True)) if el else ""
    # 폴백: title 태그에서 추출
    if not shop_name and soup.title and soup.title.string:
        shop_name = re.sub(r'^\[카닥\]\s*', '', soup.title.string).strip()

    # 2. 위치 추출 (파일명 우선, 실패 시 HTML 본문에서 탐색)
    location = address_from_filename(path)
    if not location:
        text_lines = [ln.strip() for ln in soup.get_text("\n", strip=True).splitlines() if ln.strip()]
        for ln in text_lines:
            m = KOREAN_ADDR_RE.search(ln)
            if m:
                location = m.group(0).strip()
                break
        if not location: # 주소 패턴으로 못찾을 경우 키워드 기반으로 재탐색
            for ln in text_lines:
                if any(p in ln for p in ['서울','경기','인천','부산','대구','광주','대전','울산','세종','제주','강원','충북','충남','전북','전남','경북','경남']):
                    if any(tok in ln for tok in ['구','시','군']):
                        location = ln.strip()
                        break

    # 3. 리뷰 목록 파싱
    reviews = parse_reviews(html)

    # 4. 원본 URL 재구성 (파일 경로에서 UUID 추출)
    m = UUID_RE.search(path)
    source_url = f"https://repair.cardoc.co.kr/shops/{m.group(1)}" if m else None

    # 5. 최종 JSON 객체 생성
    output = {
        "정비소명": shop_name or "",
        "위치": location or "",
        "리뷰": reviews,
        "source_url": source_url
    }
    return json.dumps(output, ensure_ascii=False)

def main():
    """Spark Job을 실행하는 메인 함수입니다."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="S3의 원본 HTML 파일 경로(글롭 패턴 포함)")
    parser.add_argument("--output-path", required=True, help="S3에 저장할 Parquet/JSONL 경로")
    parser.add_argument("--format", default="parquet", choices=["jsonl", "parquet"], help="출력 형식")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Cardoc_Shop_Parser").getOrCreate()
    
    # (파일경로, 파일내용) RDD 생성
    rdd = spark.sparkContext.wholeTextFiles(args.input_path)

    # 각 파일을 파싱하여 JSON 문자열 RDD 생성
    json_rdd = rdd.map(lambda rec: build_shop_json(rec[0], rec[1]))

    if args.format == "jsonl":
        json_rdd.saveAsTextFile(args.output_path)
    else: # parquet
        # JSON RDD를 직접 DataFrame으로 변환하여 Parquet으로 저장
        # Spark가 JSON 스키마를 자동으로 추론
        df = spark.read.json(json_rdd)
        df.write.mode("overwrite").parquet(args.output_path)

    spark.stop()

if __name__ == "__main__":
    main()