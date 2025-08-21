import time
import random
import yaml
import boto3
from pathlib import Path
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from airflow.hooks.base import BaseHook
# ✨ S3Hook을 import 합니다.
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- 유틸리티 함수 ---

def setup_driver(config):
    ua = random.choice(config['fetcher']['selenium']['user_agents'])
    opts = Options()
    opts.page_load_strategy = "eager"
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=ko-KR")
    opts.add_argument("--window-size=420,860")
    opts.add_argument(f"--user-agent={ua}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument("--disable-blink-features=AutomationControlled")

    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
    }
    opts.add_experimental_option("prefs", prefs)

    driver = webdriver.Remote(
        command_executor='http://selenium-hub:4444/wd/hub',
        options=opts
    )
    return driver

def safe_click(drv, el):
    try:
        drv.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(random.uniform(0.05, 0.1))
        el.click()
        return True
    except Exception:
        try:
            drv.execute_script("arguments[0].click();", el)
            return True
        except Exception:
            return False

def find_element_by_xpaths(drv, xpaths):
    for xp in xpaths:
        try:
            el = drv.find_element(By.XPATH, xp)
            if el:
                return el
        except Exception:
            continue
    return None

# --- Fetcher 로직 ---

def load_all_products(driver, config):
    """
    '더보기' 버튼이 더 이상 없을 때까지 계속 클릭하여 모든 상품 목록을 로드합니다.
    """
    load_more_xpaths = config['fetcher']['xpaths']['load_more']
    while True:
        try:
            load_more_button = find_element_by_xpaths(driver, load_more_xpaths)
            if load_more_button and load_more_button.is_displayed():
                safe_click(driver, load_more_button)
                # 새 콘텐츠가 로드될 때까지 대기
                time.sleep(random.uniform(1.5, 2.5))
            else:
                print("No more 'load more' button found or it's not visible. Assuming all products are loaded.")
                break
        except Exception as e:
            print(f"An error occurred while trying to click 'load more', or finished loading: {e}")
            break


# ✨ s3_client 대신 s3_hook을 받도록 수정
def save_html_to_s3(s3_hook, bucket, prefix, category, html_content):
    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    key = f"{prefix}/{category}/{category}_{now}.html"
    try:
        # ✨ S3Hook의 load_string 메서드를 사용하여 업로드
        s3_hook.load_string(
            string_data=html_content,
            key=key,
            bucket_name=bucket,
            replace=True
        )
        print(f"Successfully saved HTML to s3://{bucket}/{key}")
    except Exception as e:
        print(f"Failed to save HTML to S3. Error: {e}")

# ✨ s3_client 대신 s3_hook을 받도록 수정
def crawl_and_save(config, s3_hook):
    driver = setup_driver(config)
    base_url = config['fetcher']['site']['base_url']
    categories = config['fetcher']['site']['target_categories']
    s3_bucket = config['s3']['source_bucket']
    s3_prefix = config['s3']['source_prefix']

    try:
        for category in categories:
            print(f"--- Processing Category: {category} ---")
            driver.get(base_url)
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1)

            btn_all_xpaths = config['fetcher']['xpaths']['btn_all']
            all_products_btn = find_element_by_xpaths(driver, btn_all_xpaths)
            if not all_products_btn or not safe_click(driver, all_products_btn):
                print("Could not find or click '제품전체보기' button. Skipping category.")
                continue
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1)

            cat_xpath = config['fetcher']['xpaths']['category_template'].format(cat=category)
            try:
                cat_link = WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, cat_xpath)))
                safe_click(driver, cat_link)
                WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(1)
            except TimeoutException:
                print(f"Could not find or click category link for '{category}'. Skipping.")
                continue

            load_all_products(driver, config)

            final_html = driver.page_source
            # ✨ s3_hook을 전달
            save_html_to_s3(s3_hook, s3_bucket, s3_prefix, category, final_html)

    finally:
        driver.quit()

# --- Airflow PythonOperator 호출 함수 ---

def run_downloader(config_path: str):
    """
    설정 파일을 읽고 S3에 연결한 뒤, 크롤러를 실행하는
    Airflow PythonOperator 호출 함수입니다.
    """
    print(f"Loading configuration from {config_path}")
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    aws_conn_id = config['s3']['aws_conn_id']
    print(f"Getting AWS connection '{aws_conn_id}'")
    
    # ✨ boto3.client 대신 S3Hook을 생성합니다.
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    print("Starting crawler and uploader...")
    # ✨ 생성된 s3_hook을 전달합니다.
    crawl_and_save(config, s3_hook)
    print("Finished crawler and uploader.")

if __name__ == "__main__":
    # 로컬 테스트용
    print("This script is designed to be run from an Airflow DAG.")
