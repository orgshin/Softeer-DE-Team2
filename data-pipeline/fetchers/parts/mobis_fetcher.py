# ==============================================================================
# etl_pipelines/parts/fetchers/mobis_fetcher.py
#
# 현대모비스 부품 정보 수집 (Fetcher) 스크립트
# - Selenium을 사용하여 동적 웹 페이지의 부품 정보를 스크래핑합니다.
# - 제조사, 차종, 모델별로 검색 키워드를 순회하며 HTML을 수집합니다.
# ==============================================================================
import logging
import random
import re
import time
import yaml
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# ==============================================================================
# Selenium 유틸리티 함수
# ==============================================================================

def jsclick(driver, element):
    """자바스크립트를 사용하여 엘리먼트를 클릭합니다."""
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", element)
    driver.execute_script("arguments[0].click();", element)

def safe_filename(s: str) -> str:
    """
    파일/폴더 이름으로 허용된 문자(영문, 숫자, 한글, -, _, .)를 제외한
    모든 특수문자를 '_'로 치환하여 안전한 파일명을 생성합니다.
    """
    # 허용할 문자 외의 모든 것을 '_'로 치환
    s = re.sub(r'[^a-zA-Z0-9가-힣\-._]', '_', s)
    return s

def wait_idle(driver, timeout: int = 10):
    """페이지 로딩(isRun='N')이 완료될 때까지 대기합니다."""
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            status = driver.find_element(By.ID, "isRun").get_attribute("value").upper()
            if status == "N":
                return True
        except Exception:
            # 엘리먼트가 없는 경우도 작업이 끝난 것으로 간주
            return True
        time.sleep(0.1)
    return False

def ensure_selection(driver, maker: str, vtype: str, wait: WebDriverWait):
    """제조사(현대/기아)와 차종(승용/상용)이 올바르게 선택되었는지 확인하고 클릭합니다."""
    make_id = "make1" if maker == "현대" else "make2"
    use_id = "use1" if vtype == "승용" else "use2"

    if not driver.find_element(By.ID, make_id).is_selected():
        label = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"label[for='{make_id}']")))
        jsclick(driver, label)
        time.sleep(0.5)

    if not driver.find_element(By.ID, use_id).is_selected():
        label = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"label[for='{use_id}']")))
        jsclick(driver, label)
        time.sleep(0.5)

# ==============================================================================
# Airflow PythonOperator가 호출할 메인 실행 함수
# ==============================================================================

def run_fetcher(config: dict):
    """
    설정 파일을 기반으로 Selenium을 실행하여 Mobis 부품 정보 페이지의 HTML을
    수집하고 S3에 업로드하는 메인 함수입니다.
    """
    cfg = config
    s3_cfg = cfg["s3"]
    fetcher_cfg = cfg["fetcher"]

    s3_hook = S3Hook(aws_conn_id=s3_cfg["aws_conn_id"])
    s3_bucket = s3_cfg["source_bucket"]

    opts = webdriver.ChromeOptions()
    opts.add_argument(f"user-agent={random.choice(fetcher_cfg['chrome_options']['user_agents'])}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    if fetcher_cfg['chrome_options'].get("headless", True):
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")

    try:
        driver = webdriver.Remote(command_executor='http://selenium-hub:4444/wd/hub', options=opts)
        wait = WebDriverWait(driver, 20)
        logging.info("Selenium Hub에 성공적으로 연결되었습니다.")
    except Exception as e:
        logging.error(f"Selenium Hub 연결에 실패했습니다: {e}")
        raise

    IS_TEST_MODE = True # 테스트용 플래그

    # --- 2. 데이터 수집 로직 ---
    try:
        for maker in fetcher_cfg["manufacturers"]:
            for vtype in fetcher_cfg["vehicle_types"]:
                logging.info(f"==> 제조사: {maker}, 차종: {vtype} 수집 시작")
                driver.get(fetcher_cfg["base_url"])
                ensure_selection(driver, maker, vtype, wait)

                # 모델 목록이 로드될 때까지 대기
                sel_el = wait.until(EC.presence_of_element_located((By.ID, "model")))
                sel = Select(sel_el)
                WebDriverWait(driver, 10).until(
                    lambda d: len([o for o in sel.options if o.text.strip() not in ("전체", "선택", "")]) >= 1
                )
                models = [o.text.strip() for o in sel.options if o.text.strip() not in ("전체", "선택", "")]
                logging.info(f"수집 대상 모델: {models}")

                for model_name in models:
                    driver.get(fetcher_cfg["base_url"])
                    ensure_selection(driver, maker, vtype, wait)
                    Select(wait.until(EC.element_to_be_clickable((By.ID, "model")))).select_by_visible_text(model_name)
                    time.sleep(1)

                    for kw in fetcher_cfg["part_keywords"]:
                        logging.info(f"-----> 모델: {model_name}, 키워드: '{kw}' 검색 시작")
                        kw_input = wait.until(EC.visibility_of_element_located((By.ID, "searchNm")))
                        kw_input.clear(); kw_input.send_keys(kw)
                        jsclick(driver, wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".btn-wrap > .btn-red"))))
                        wait_idle(driver, timeout=fetcher_cfg["timeouts"]["idle_timeout"])

                        page = 1
                        while True:
                            html_content = driver.page_source
                            if "조회된 데이터가 없습니다" in html_content:
                                logging.info(f"'{kw}'에 대한 데이터가 없어 다음 키워드로 넘어갑니다.")
                                break

                            # S3에 저장할 경로 및 파일명 생성
                            safe_maker = safe_filename(maker)
                            safe_vtype = safe_filename(vtype)
                            safe_model = safe_filename(model_name)
                            filename = f"{safe_filename(kw)}__page{page}.html"
                            s3_key = f"parts/mobis_parts/{safe_maker}/{safe_vtype}/{safe_model}/{filename}"

                            # S3에 HTML 업로드
                            s3_hook.load_string(string_data=html_content, key=s3_key, bucket_name=s3_bucket, replace=True)
                            logging.info(f"S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                            if IS_TEST_MODE:
                                logging.warning("### 테스트 모드: 첫 페이지만 수집하고 다음으로 넘어갑니다. ###")
                                break
                            
                            # 다음 페이지로 이동
                            try:
                                current_page_num = int(driver.find_element(By.CSS_SELECTOR, "#table-wrap li.on a").text)
                                next_page_link = driver.find_element(By.XPATH, f"//a[text()='{current_page_num + 1}']")
                                jsclick(driver, next_page_link)
                                wait_idle(driver, timeout=fetcher_cfg["timeouts"]["idle_timeout"])
                                page += 1
                                time.sleep(random.uniform(*fetcher_cfg['cooldowns']['page_pause']))
                            except Exception:
                                logging.info("마지막 페이지이거나 다음 페이지를 찾을 수 없습니다.")
                                break
                        
                        if IS_TEST_MODE: break
                    if IS_TEST_MODE: break
                if IS_TEST_MODE: break
            if IS_TEST_MODE: break

    except Exception as e:
        logging.error(f"크롤링 중 심각한 오류 발생: {e}", exc_info=True)
    finally:
        logging.info("크롤링 작업을 모두 마쳤습니다. 드라이버를 종료합니다.")
        if 'driver' in locals() and driver:
            driver.quit()