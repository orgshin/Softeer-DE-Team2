# /opt/bitnami/spark/work/parsers/repair_shop/cardoc_parser.py
# -*- coding: utf-8 -*-
# 실행 예)
# spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4 \
#   /opt/bitnami/spark/work/parsers/repair_shop/cardoc_parser.py \
#   --input-path s3a://team2-html-raw/repair_shop/html/20250823/ \
#   --output-path s3a://team2-html-out/repair_shop/parquet/20250823

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

def _textlines(soup):
    txt = soup.get_text("\n", strip=True)
    lines = [ln.strip() for ln in txt.splitlines()]
    return [ln for ln in lines if ln]

def parse_shop_name(soup):
    el = soup.select_one("header .text-lg")
    if el:
        name = el.get_text(strip=True)
        if name:
            return name
    if soup.title and soup.title.string:
        t = soup.title.string.strip()
        t = re.sub(r'^\[카닥\]\s*', '', t).strip()
        return t or None
    return None

def parse_address(soup):
    candidates = []
    for ln in _textlines(soup):
        if KOREAN_ADDR_RE.search(ln):
            candidates.append(ln)
    candidates = sorted(set(candidates), key=len, reverse=True)
    return candidates[0] if candidates else None

def parse_rating_summary_and_review_count(soup):
    rating = None
    review_count = None
    for ln in _textlines(soup):
        m = RATING_5_RE.search(ln)
        if m:
            try:
                rating = float(m.group(1))
                break
            except:
                pass
    lines = _textlines(soup)
    for i, ln in enumerate(lines):
        if "리뷰" in ln or "리뷰수" in ln or "리뷰 개수" in ln:
            window = [ln]
            if i > 0: window.append(lines[i-1])
            if i+1 < len(lines): window.append(lines[i+1])
            for w in window:
                for m in INT_RE.finditer(w):
                    try:
                        n = int(m.group(0))
                        if 0 < n < 100000:
                            review_count = n
                            break
                    except:
                        pass
            if review_count is not None:
                break
    return rating, review_count

def parse_enhanced_reviews(html_text: str):
    from bs4 import BeautifulSoup
    import json, re

    soup = BeautifulSoup(html_text, "lxml")

    def norm(s):
        if not s: return None
        s = re.sub(r'\s+', ' ', s).strip()
        return s

    def count_star_svgs(container):
        yellow = 0
        for svg in container.find_all("svg"):
            path = svg.find("path")
            fill = (path.get("fill") if path else None) or svg.get("fill") or ""
            if "#FFCA35" in fill.upper():
                yellow += 1
        return yellow if yellow > 0 else None

    records = []

    # ★ 리뷰 카드를 명시적으로 순회 (중복 원천 차단)
    for card in soup.select("div.py-5.space-y-4"):
        # 카드 내부에서만 찾는다
        stars_row   = card.select_one("div.flex.items-center.space-x-1")
        aspects_row = card.select_one("div.flex.text-sm.text-gray-800.font-medium.flex-wrap")
        meta_row    = card.select_one("div.flex.space-x-1.text-sm.text-gray-800.font-medium")

        rate = count_star_svgs(stars_row) if stars_row else None
        work_meta = norm(meta_row.get_text(" ", strip=True)) if meta_row else None

        aspects = {}
        if aspects_row:
            pairs = aspects_row.select("div.flex.items-center.space-x-1.whitespace-nowrap")
            if pairs:
                for p in pairs:
                    spans = p.find_all("span")
                    if len(spans) >= 2:
                        aspects[norm(spans[0].get_text(strip=True))] = norm(spans[1].get_text(strip=True))
            else:
                spans = aspects_row.find_all("span")
                for i in range(0, len(spans)-1, 2):
                    aspects[norm(spans[i].get_text(strip=True))] = norm(spans[i+1].get_text(strip=True))
        if not aspects:
            aspects = None

        # content: 카드 안에서 메타/아스펙트 다음에 나오는 ‘본문성’ 텍스트
        def next_long_text(anchor):
            if not anchor: return None
            nxt = anchor.find_next(lambda t: t.name in ("div", "p"))
            hops = 0
            while nxt and hops < 12:
                txt = nxt.get_text(" ", strip=True)
                t = norm(txt)
                if t and len(t) > 15 and all(k not in t for k in ["수리퀄리티", "친절도", "견적 정확도"]):
                    # 메타성 문구 제외
                    if "·" in t and ("소요" in t or "보험" in t):
                        nxt = nxt.find_next(lambda t: t.name in ("div","p")); hops += 1; continue
                    # 사장님 답글/매장 공지성 한 줄 필터 (원하면 더 강하게)
                    if t.startswith("영등포 자동차 정비검사소 · ") and "좋은후기 감사" in t:
                        nxt = nxt.find_next(lambda t: t.name in ("div","p")); hops += 1; continue
                    return t
                nxt = nxt.find_next(lambda t: t.name in ("div","p")); hops += 1
            return None

        content = next_long_text(aspects_row) or next_long_text(meta_row) or next_long_text(stars_row) or None

        if any([rate is not None, work_meta, aspects, content]):
            records.append({
                "rate": rate,
                "work_meta": work_meta,
                "aspects": aspects,
                "content": content
            })

    # ★ 중복 제거 (work_meta+content 기준) + rate가 있는 쪽을 보존
    by_key = {}
    for r in records:
        key = (norm(r.get("work_meta")), norm(r.get("content")))
        keep = by_key.get(key)
        if keep is None:
            by_key[key] = r
        else:
            # 둘 다 있으면 rate가 있는 쪽 / 더 높은 rate를 우선
            new_rate = r.get("rate")
            old_rate = keep.get("rate")
            if (old_rate is None and new_rate is not None) or (new_rate or 0) > (old_rate or 0):
                # aspects도 비어있던 게 새로 채워졌다면 합쳐준다
                if not keep.get("aspects") and r.get("aspects"):
                    keep["aspects"] = r["aspects"]
                keep["rate"] = new_rate
    return list(by_key.values())

def extract_shop_id_from_path(path_str: str):
    m = UUID_RE.search(path_str)
    return m.group(1) if m else None

# ---------- Spark main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-path", required=True, help="S3A 입력 경로(디렉터리/글롭)")
    ap.add_argument("--output-path", required=True, help="S3A 출력 디렉터리(JSON)")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("CardocShopParser")
             .getOrCreate())
    sc = spark.sparkContext

    # (path, html) 로 읽기
    rdd = sc.wholeTextFiles(args.input_path)

    def parse_one(record):
        path, html = record
        soup = BeautifulSoup(html, "lxml")

        shop_name = parse_shop_name(soup)
        address = parse_address(soup)
        rating_summary, review_count = parse_rating_summary_and_review_count(soup)
        reviews = parse_enhanced_reviews(html)

        shop_id = extract_shop_id_from_path(path)
        source_url = f"https://repair.cardoc.co.kr/shops/{shop_id}" if shop_id else None

        if review_count is None:
            review_count = len(reviews) if reviews else None

        return json.dumps({
            "shop_id": shop_id,
            "shop_name": shop_name,
            "address": address,
            "rating_summary": rating_summary,
            "review_count": review_count,
            "reviews": reviews,             # Array[Struct]
            "source_url": source_url,
            "source_path": path
        }, ensure_ascii=False)

    jsonl_rdd = rdd.map(parse_one)
    df = spark.read.json(jsonl_rdd)

    # 결과 JSON(파티션별 JSONL) 저장 — 기존 prefix 형식 유지
    df.write.mode("overwrite").parquet(args.output_path)

    spark.stop()

if __name__ == "__main__":
    main()
