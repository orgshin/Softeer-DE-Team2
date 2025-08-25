# ==============================================================================
# etl_pipelines/parts/fetchers/partsro_fetcher.py
#
# 파츠로 부품 정보 수집 (Fetcher) 스크립트
# - 모바일 페이지를 타겟으로 Selenium을 사용하여 '더보기'를 끝까지 클릭합니다.
# - 최종적으로 로드된 전체 페이지의 HTML을 카테고리별로 S3에 저장합니다.
# ==============================================================================
import time
import random
import yaml
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

TEST_CODE=True

# ==============================================================================
# Selenium 유틸리티 함수
# ==============================================================================

def setup_driver(config: dict):
    """최적화된 설정으로 Selenium Remote WebDriver를 생성합니다."""
    ua = random.choice(config['selenium']['user_agents'])
    opts = Options()
    opts.page_load_strategy = "eager"
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--window-size=420,860") # 모바일 사이즈
    opts.add_argument(f"--user-agent={ua}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")

    # 이미지, CSS 로드를 비활성화하여 속도 향상
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Remote(command_executor='http://selenium-hub:4444/wd/hub', options=opts)
    return driver

def safe_click(driver, element):
    """엘리먼트를 안전하게 클릭합니다 (JavaScript 클릭 포함)."""
    try:
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
        time.sleep(random.uniform(0.05, 0.1))
        element.click()
        return True
    except Exception:
        try:
            # 일반 클릭 실패 시 JavaScript 클릭 재시도
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception:
            return False

def find_element_by_xpaths(driver, xpaths: list):
    """여러 XPath 후보 중 먼저 찾아지는 엘리먼트를 반환합니다."""
    for xp in xpaths:
        try:
            element = driver.find_element(By.XPATH, xp)
            if element:
                return element
        except Exception:
            continue
    return None

def load_all_products(driver, config: dict):
    """'더보기' 버튼이 사라질 때까지 계속 클릭하여 모든 상품을 로드합니다."""
    load_more_xpaths = config['fetcher']['xpaths']['load_more']
    while True:
        try:
            load_more_button = find_element_by_xpaths(driver, load_more_xpaths)
            if load_more_button and load_more_button.is_displayed():
                safe_click(driver, load_more_button)
                time.sleep(random.uniform(1.5, 2.5)) # 새 콘텐츠 로딩 대기
            else:
                print("더보기 버튼이 없거나 보이지 않아 전체 상품 로드를 완료합니다.")
                break
        except Exception as e:
            print(f"더보기 버튼 클릭 중 오류가 발생했거나 로딩이 완료되었습니다: {e}")
            break
        if TEST_CODE: break

def save_html_to_s3(s3_hook: S3Hook, bucket: str, prefix: str, category: str, html_content: str):
    """수집된 HTML을 S3에 저장합니다."""
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}/{category}/{category}_{now}.html"
    try:
        s3_hook.load_string(string_data=html_content, key=key, bucket_name=bucket, replace=True)
        print(f"HTML 저장 성공: s3://{bucket}/{key}")
    except Exception as e:
        print(f"HTML 저장 실패. 오류: {e}")

# ==============================================================================
# 메인 크롤링 로직
# ==============================================================================

def crawl_and_save(config: dict, s3_hook: S3Hook):
    """
    카테고리별로 순회하며 상품 목록 HTML을 수집하고 S3에 저장하는 함수입니다.
    """
    driver = setup_driver(config)
    fetcher_cfg = config['fetcher']
    s3_cfg = config['s3']
    base_url = fetcher_cfg['site']['base_url']
    categories = fetcher_cfg['site']['target_categories']
    s3_bucket = s3_cfg['source_bucket']
    s3_prefix = s3_cfg['source_prefix']

    try:
        for category in categories:
            print(f"--- 카테고리 처리 시작: {category} ---")
            driver.get(base_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1)

            # '제품전체보기' 버튼 클릭
            btn_all_xpaths = fetcher_cfg['xpaths']['btn_all']
            all_products_btn = find_element_by_xpaths(driver, btn_all_xpaths)
            if not all_products_btn or not safe_click(driver, all_products_btn):
                print("'제품전체보기' 버튼을 찾거나 클릭할 수 없어 카테고리를 건너뜁니다.")
                continue
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1)

            # 대상 카테고리 링크 클릭
            cat_xpath = fetcher_cfg['xpaths']['category_template'].format(cat=category)
            try:
                cat_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, cat_xpath)))
                safe_click(driver, cat_link)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(1)
            except TimeoutException:
                print(f"카테고리 '{category}' 링크를 찾거나 클릭할 수 없어 건너뜁니다.")
                continue

            # 모든 제품 로드 ('더보기' 반복 클릭)
            load_all_products(driver, config)

            # 최종 HTML을 S3에 저장
            final_html = driver.page_source
            save_html_to_s3(s3_hook, s3_bucket, s3_prefix, category, final_html)
            if TEST_CODE: break

    finally:
        driver.quit()

# ==============================================================================
# Airflow PythonOperator가 호출할 메인 실행 함수
# ==============================================================================

def run_downloader(config: dict):
    """
    Airflow에서 호출하는 엔트리포인트 함수.
    설정 파일을 로드하고 S3 Hook을 생성하여 크롤러를 실행합니다.
    """

    aws_conn_id = config['s3']['aws_conn_id']
    print(f"AWS Connection '{aws_conn_id}' 가져오는 중")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    print("크롤러 및 업로더 시작...")
    crawl_and_save(config, s3_hook)
    print("크롤러 및 업로더 완료.")