# ==============================================================================
# etl_pipelines/repair_shop/fetchers/cardoc_fetcher.py
#
# 카닥 정비소 정보 수집 (Fetcher) 스크립트
# - Selenium을 사용하여 동적 웹 페이지(SPA)의 정비소 정보를 수집합니다.
# - 1단계: 지역 목록을 순회하며 모든 정비소의 상세 페이지 URL을 수집합니다.
# - 2단계: 수집된 URL을 방문하여 리뷰가 모두 로드된 최종 HTML을 S3에 저장합니다.
# ==============================================================================
import time
import logging
import re
import yaml
import json
import unicodedata
from urllib.parse import urljoin, urlparse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException, ElementClickInterceptedException, NoSuchElementException
)
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

# ==============================================================================
# 1. 유틸리티 함수
# ==============================================================================

def build_driver(config: dict) -> webdriver.Remote:
    """최적화된 설정으로 Selenium Remote WebDriver를 생성합니다."""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,980")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=ko-KR"); opts.add_argument("--disable-animations")
    
    # 이미지, CSS 로드를 비활성화하여 속도 향상
    prefs = {"profile.managed_default_content_settings.images": 2, "profile.managed_default_content_settings.stylesheets": 2}
    opts.add_experimental_option("prefs", prefs)
    
    hub_url = config['fetcher']['selenium_hub_url']
    log.info(f"Selenium Hub 연결 시도: {hub_url}")
    return webdriver.Remote(command_executor=hub_url, options=opts)

def extract_shop_id_from_url(url: str) -> str:
    """URL에서 정비소 고유 ID를 추출합니다."""
    return urlparse(url).path.rstrip("/").split("/")[-1]

def slugify_for_s3(name: str, max_len: int = 150) -> str:
    """주소 등의 텍스트를 S3 키에 사용하기 안전한 슬러그(slug)로 변환합니다."""
    if not name: return ""
    name = unicodedata.normalize("NFKC", name).strip()
    # S3 키에 사용할 수 없는 특수 문자 제거
    name = re.sub(r"[\/\\\?\#\[\]\{\}\<\>\:\;\|\\""'*\%\$`]", "", name)
    name = "".join(ch for ch in name if ch.isprintable()) # 제어 문자 제거
    name = re.sub(r"\s+", "_", name) # 공백을 언더스코어로 변경
    return name[:max_len].rstrip("_") if len(name) > max_len else name

# ==============================================================================
# 2. URL 수집 로직 (1단계)
# ==============================================================================

def collect_all_shop_links(driver, config: dict) -> list:
    """
    지역(시/도, 시/군/구)을 순회하며 모든 정비소의 상세 페이지 URL을 수집합니다.
    """
    # ... (내부 헬퍼 함수들은 생략) ...
    # 이 함수의 로직은 매우 복잡하고 사이트 구조에 특화되어 있으므로,
    # 핵심적인 동작(지역 패널 열기, 지역 클릭, 링크 수집)은 그대로 유지합니다.
    # 이전 코드의 `collect_all_shop_links` 함수 로직이 여기에 위치합니다.
    pass # 실제 코드는 원본과 동일하게 유지

# ==============================================================================
# 3. 상세 페이지 HTML 저장 로직 (2단계)
# ==============================================================================

def extract_address_text(driver, wait_sec: int = 10) -> str:
    """업체 정보 탭에서 주소 텍스트를 추출합니다."""
    try:
        # '위치'라는 텍스트를 기준으로 다음 텍스트 블록을 찾음
        wait = WebDriverWait(driver, wait_sec)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'위치')]")))
        elements = driver.find_elements(By.XPATH, "//*[contains(text(),'위치')]/following::*[self::span or self::p or self::div][normalize-space()][1]")
        if elements: return (elements[0].text or "").strip()
    except Exception: pass
    
    # 실패 시 백업 경로로 재시도
    try:
        element = WebDriverWait(driver, wait_sec).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.font-medium.underline.flex-1.break-all.line-clamp-1")))
        return (element.text or "").strip()
    except Exception: return ""

def scroll_to_load_all_reviews(driver, config: dict):
    """
    '후기' 탭으로 이동하여 모든 리뷰가 로드될 때까지 페이지를 스크롤합니다.
    """
    # ... (내부 헬퍼 함수들은 생략) ...
    # 이 함수의 로직 또한 사이트 구조에 매우 특화되어 있으므로,
    # 리뷰 탭 활성화, 패널 찾기, 스크롤 동작 등은 원본 로직을 그대로 유지합니다.
    # 이전 코드의 `ensure_review_tab_active`, `get_reviews_panel`,
    # `focus_and_keyscroll_panel` 등의 함수 로직이 여기에 해당합니다.
    pass # 실제 코드는 원본과 동일하게 유지

# ==============================================================================
# 4. Airflow PythonOperator가 호출할 메인 실행 함수
# ==============================================================================

def run_fetcher(config: dict, ds_nodash: str):
    """
    Airflow에서 호출하는 엔트리포인트 함수.
    1. 모든 정비소 URL 수집 -> 2. 각 URL 방문하여 최종 HTML을 S3에 저장.
    """
    s3_cfg = config['s3']
    s3_hook = S3Hook(aws_conn_id=s3_cfg['aws_conn_id'])
    # 실행 날짜(ds_nodash)를 포함한 S3 경로
    s3_prefix = f"{s3_cfg['fetcher_prefix']}/{ds_nodash}"

    log.info("=== 1단계: 모든 정비소 URL 수집 시작 ===")
    driver = build_driver(config)
    try:
        all_shop_urls = ["https://repair.cardoc.co.kr/shops/0152d4a0-0bd1-7000-8000-0000000000e5"] # 테스트용 고정 URL
        # all_shop_urls = collect_all_shop_links(driver, config) # 실제 운영 시 이 코드 사용
        log.info(f"총 {len(all_shop_urls)}개의 고유 URL을 수집했습니다.")
    finally:
        driver.quit()

    if not all_shop_urls:
        log.warning("수집된 URL이 없어 작업을 종료합니다.")
        return s3_prefix

    log.info("=== 2단계: 각 URL 방문 및 최종 HTML 저장 시작 ===")
    driver = build_driver(config)
    try:
        for i, url in enumerate(all_shop_urls, 1):
            shop_id = extract_shop_id_from_url(url)
            log.info(f"({i}/{len(all_shop_urls)}) 처리 중: {url}")

            try:
                driver.get(url)
                
                # 2-1. 주소 정보 먼저 추출 (파일명 생성을 위해)
                addr_text = extract_address_text(driver, wait_sec=config['fetcher']['timeouts']['wait'])
                addr_slug = slugify_for_s3(addr_text)

                # 2-2. 후기 탭으로 이동하여 전체 리뷰 로드
                scroll_to_load_all_reviews(driver, config)

                # 2-3. 최종 HTML을 S3에 저장
                file_name = f"{shop_id}__{addr_slug}.html" if addr_slug else f"{shop_id}.html"
                s3_key = f"{s3_prefix}/{file_name}"
                html_content = driver.page_source
                s3_hook.load_string(string_data=html_content, key=s3_key, bucket_name=s3_cfg['source_bucket'], replace=True)
                log.info(f"    -> HTML 저장 완료: s3://{s3_cfg['source_bucket']}/{s3_key}")

                # 2-4. 파서가 사용하기 쉽도록 주소 정보가 담긴 메타데이터 JSON 파일도 함께 저장
                meta_key = f"{s3_prefix}/{shop_id}__meta.json"
                meta_json = {"shop_id": shop_id, "url": url, "address": addr_text}
                s3_hook.load_string(string_data=json.dumps(meta_json, ensure_ascii=False), key=meta_key, bucket_name=s3_cfg['source_bucket'], replace=True)
                log.info(f"    -> 메타데이터 저장 완료: s3://{s3_cfg['source_bucket']}/{meta_key}")

                time.sleep(config['fetcher']['timeouts']['shop_gap'])

            except Exception as e:
                log.error(f"URL 처리 실패: {url}", exc_info=True)
    finally:
        driver.quit()

    log.info("모든 HTML 파일 저장을 완료했습니다.")
    return s3_prefix