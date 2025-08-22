import logging
import random
import time
import re
import yaml
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# --- 유틸리티 함수 ---
def jsclick(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)

# ▼▼▼ [핵심 수정] 절대로 실패하지 않는 방식으로 함수를 완전히 재작성합니다. ▼▼▼
def safe_filename(s: str) -> str:
    """
    파일/폴더 이름으로 허용된 문자(영문, 숫자, 한글, -, _, .)를 제외한
    모든 특수 문자(콜론 ':' 포함)를 '_'로 확실하게 치환하는 함수.
    """
    # 허용할 문자 외의 모든 것을 '_'로 치환합니다.
    # [^...]: 괄호 안의 문자를 제외한 모든 문자와 매치
    # a-zA-Z0-9: 모든 영문 알파벳과 숫자
    # 가-힣: 모든 한글 글자
    # \-_. : 하이픈, 언더스코어, 마침표
    s = re.sub(r'[^a-zA-Z0-9가-힣\-._]', '_', s)
    return s
# ▲▲▲ [핵심 수정] ▲▲▲

def wait_idle(driver, timeout=10):
    end = time.time() + timeout
    while time.time() < end:
        try:
            st = driver.find_element(By.ID, "isRun").get_attribute("value").upper()
            if st == "N": return True
        except Exception: return True
        time.sleep(0.1)
    return False

def ensure_selection(driver, maker, vtype, wait):
    make_id = "make1" if maker == "현대" else "make2"
    use_id = "use1" if vtype == "승용" else "use2"
    if not driver.find_element(By.ID, make_id).is_selected():
        lbl = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"label[for='{make_id}']")))
        jsclick(driver, lbl); time.sleep(0.5)
    if not driver.find_element(By.ID, use_id).is_selected():
        lbl = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"label[for='{use_id}']")))
        jsclick(driver, lbl); time.sleep(0.5)

# --- Airflow PythonOperator가 호출할 메인 함수 ---
def run_fetcher(config_path: str):
    # (이하 로직은 이전과 동일하며, 수정된 safe_filename 함수가 적용됩니다)
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

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
        opts.add_argument("--window-size=1920x1080")

    try:
        driver = webdriver.Remote(
            command_executor='http://selenium-hub:4444/wd/hub',
            options=opts
        )
        wait = WebDriverWait(driver, 20)
        logging.info("Selenium Hub에 성공적으로 연결되었습니다.")
    except Exception as e:
        logging.error(f"Selenium Hub 연결에 실패했습니다: {e}")
        raise

    IS_TEST_MODE = True

    try:
        for maker in fetcher_cfg["manufacturers"]:
            for vtype in fetcher_cfg["vehicle_types"]:
                logging.info(f"==> 제조사: {maker}, 차종: {vtype} 수집 시작")
                driver.get(fetcher_cfg["base_url"])
                ensure_selection(driver, maker, vtype, wait)
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
                        kw_input.clear()
                        kw_input.send_keys(kw)
                        jsclick(driver, wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".btn-wrap > .btn-red"))))
                        wait_idle(driver, timeout=fetcher_cfg["timeouts"]["idle_timeout"])

                        page = 1
                        while True:
                            html_content = driver.page_source
                            if "조회된 데이터가 없습니다" in html_content:
                                logging.info(f"'{kw}'에 대한 데이터가 없어 다음 키워드로 넘어갑니다.")
                                break

                            safe_maker = safe_filename(maker)
                            safe_vtype = safe_filename(vtype)
                            # 재작성된 safe_filename 함수가 여기서 ':'을 확실하게 제거합니다.
                            safe_model = safe_filename(model_name)
                            filename = f"{safe_filename(kw)}__page{page}.html"
                            s3_key = f"parts/mobis_parts/{safe_maker}/{safe_vtype}/{safe_model}/{filename}"

                            s3_hook.load_string(string_data=html_content, key=s3_key, bucket_name=s3_bucket, replace=True)
                            logging.info(f"S3 업로드 완료: s3://{s3_bucket}/{s3_key}")

                            if IS_TEST_MODE:
                                logging.warning("### 테스트 모드: 첫 페이지만 수집하고 다음으로 넘어갑니다. ###")
                                break
                            
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
        logging.error(f"크롤링 중 심각한 오류 발생: {e}")
    finally:
        logging.info("크롤링 작업을 모두 마쳤습니다. 드라이버를 종료합니다.")
        if 'driver' in locals() and driver:
            driver.quit()