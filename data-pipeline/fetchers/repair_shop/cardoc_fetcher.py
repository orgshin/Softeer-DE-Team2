import time
import logging
import re
import yaml
from urllib.parse import urljoin, urlparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

import unicodedata
import re

def extract_address_text(driver, wait_sec: int = 10) -> str:
    """
    기본 탭(업체 정보)에서 주소 텍스트를 추출한다.
    - 우선 '위치' 텍스트 섹션을 기준으로 다음 첫 번째 텍스트 블록을 찾음
    - 실패 시, 알려준 span 클래스 조합으로도 재시도
    """
    try:
        WebDriverWait(driver, wait_sec).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(),'위치')]"))
        )
        # '위치' 다음에 오는 첫 번째 주소성 텍스트 노드
        # (span/p 등 첫 텍스트 엘리먼트)
        cand = driver.find_elements(
            By.XPATH,
            "//*[contains(text(),'위치')]/following::*[self::span or self::p or self::div][normalize-space()][1]"
        )
        if cand:
            addr = cand[0].text.strip()
            if addr:
                return addr
    except Exception:
        pass

    # 백업 경로: 알려준 클래스 조합으로 검색
    try:
        el = WebDriverWait(driver, wait_sec).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "span.font-medium.underline.flex-1.break-all.line-clamp-1")
            )
        )
        addr = (el.text or "").strip()
        return addr
    except Exception:
        return ""


def slugify_for_s3(name: str, max_len: int = 150) -> str:
    """
    S3 key에 쓰기 안전한 슬러그 생성.
    - 공백 -> '_'
    - 슬래시/역슬래시/물음표/해시 등 위험문자 제거
    - 제어문자 제거
    - 너무 길면 컷
    """
    if not name:
        return ""

    # 정규화
    name = unicodedata.normalize("NFKC", name).strip()

    # S3에 위험한 문자 제거
    # (/, \, ?, #, [, ], {, }, <, >, :, ;, |, ", ', *, %, $, 등)
    name = re.sub(r"[\/\\\?\#\[\]\{\}\<\>\:\;\|\\""'*\%\$`]", "", name)

    # 제어문자 제거
    name = "".join(ch for ch in name if ch.isprintable())

    # 공백류 -> _
    name = re.sub(r"\s+", "_", name)

    # 너무 길면 컷
    if len(name) > max_len:
        name = name[:max_len].rstrip("_")

    return name


# ==============================================================================
# 1. 드라이버 및 공통 유틸리티
# ==============================================================================

def build_driver(config):
    """메모리 효율성을 높인 Selenium Remote WebDriver 인스턴스를 생성합니다."""
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--window-size=1400,980")
    opts.add_argument("--disable-gpu"); opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--lang=ko-KR"); opts.add_argument("--disable-animations")
    
    prefs = {
        "profile.managed_default_content_settings.images": 2,
        "profile.managed_default_content_settings.stylesheets": 2,
    }
    opts.add_experimental_option("prefs", prefs)
    
    hub_url = config['fetcher']['selenium_hub_url']
    log.info(f"Connecting to Selenium Hub at {hub_url}")
    
    return webdriver.Remote(command_executor=hub_url, options=opts)

def extract_shop_id(url):
    return urlparse(url).path.rstrip("/").split("/")[-1]

# ==============================================================================
# 2. Fetcher: 모든 정비소 URL 수집 로직 (첫 번째 코드 기반)
# ==============================================================================
def _toggle_candidates(drv):
    xps = ["//header//button[contains(@class,'items-center')]", "//button[contains(@class,'items-center') and .//span[contains(@class,'font')]]"]
    cands = []
    for xp in xps: cands += [b for b in drv.find_elements(By.XPATH, xp) if b.is_displayed()]
    return cands

def _find_cols(drv):
    containers = drv.find_elements(By.XPATH, "//div[contains(@class,'fixed')] | //div[contains(@class,'flex space-x')]")
    for c in containers:
        try:
            left = c.find_element(By.XPATH, ".//div[contains(@class,'min-w-40') and contains(@class,'overflow-y-auto')]")
            right = c.find_element(By.XPATH, ".//div[contains(@class,'w-full') and contains(@class,'overflow-y-auto')]")
            if left.is_displayed() and right.is_displayed(): return left, right
        except Exception: continue
    return None, None

def open_region_panel(drv, start_url, pause_time, retries=3):
    left, right = _find_cols(drv)
    if left and right: return left, right
    for _ in range(retries):
        for b in _toggle_candidates(drv):
            try:
                drv.execute_script("arguments[0].click();", b)
                time.sleep(pause_time)
                left, right = _find_cols(drv)
                if left and right: return left, right
            except Exception: continue
        drv.get(start_url); time.sleep(pause_time)
    raise TimeoutException("지역 선택 패널을 여는 데 실패했습니다.")

def list_texts(panel_el):
    return [b.text.strip() for b in panel_el.find_elements(By.XPATH, ".//button[normalize-space()]") if b.is_displayed()]

def click_text(panel_el, text, drv):
    for b in panel_el.find_elements(By.XPATH, ".//button[normalize-space()]"):
        if b.text.strip() == text:
            drv.execute_script("arguments[0].click();", b)
            return True
    return False

def wait_list_ready(drv, wait_time):
    WebDriverWait(drv, wait_time).until(EC.any_of(
        EC.presence_of_element_located((By.XPATH, "//section[contains(@class,'grid')]//a[@href]")),
        EC.presence_of_element_located((By.XPATH, "//*[contains(.,'아직 원하는 업체를 찾지 못했나요')]"))
    ))

def collect_links(drv, base_url, config):
    wait_list_ready(drv, config['fetcher']['timeouts']['wait'])
    seen, idle, last_count = set(), 0, 0
    max_idle = 8
    while idle < max_idle:
        links = drv.find_elements(By.CSS_SELECTOR, "a.w-full[href^='/shops/']")
        for a in links:
            try:
                href = a.get_attribute("href")
                if href: seen.add(urljoin(base_url, href))
            except StaleElementReferenceException: continue
        if len(seen) == last_count:
            idle += 1
        else:
            last_count = len(seen)
            idle = 0
        drv.execute_script("window.scrollBy(0, 1200);"); time.sleep(0.5)
    return list(seen)

def apply_region_and_wait(drv, config):
    try:
        wait_list_ready(drv, config['fetcher']['timeouts']['wait'])
        return True
    except TimeoutException:
        drv.refresh(); time.sleep(config['fetcher']['timeouts']['pause'])
        try:
            wait_list_ready(drv, config['fetcher']['timeouts']['wait'])
            return True
        except TimeoutException: return False

def collect_all_shop_links(driver, config):
    fetcher_cfg = config['fetcher']
    all_links = set()
    driver.get(fetcher_cfg['start_url']); time.sleep(fetcher_cfg['timeouts']['pause'])
    
    left_panel, _ = open_region_panel(driver, fetcher_cfg['start_url'], fetcher_cfg['timeouts']['pause'])
    left_regions = list_texts(left_panel)
    log.info(f"수집 대상 시/도: {left_regions}")

    for l_name in left_regions:
        log.info(f"--- 시/도 '{l_name}' 처리 ---")
        try:
            lp, _ = open_region_panel(driver, fetcher_cfg['start_url'], fetcher_cfg['timeouts']['pause'])
            click_text(lp, l_name, driver); time.sleep(fetcher_cfg['timeouts']['short_pause'])
            _, rp = open_region_panel(driver, fetcher_cfg['start_url'], fetcher_cfg['timeouts']['pause'])
            right_regions = list_texts(rp)
        except Exception as e:
            log.error(f"'{l_name}'의 시/군/구 목록 로드 실패: {e}"); continue
        
        for r_name in right_regions:
            log.info(f"  - 시/군/구 '{r_name}' 처리 ---")
            try:
                lp, rp = open_region_panel(driver, fetcher_cfg['start_url'], fetcher_cfg['timeouts']['pause'])
                click_text(lp, l_name, driver); time.sleep(fetcher_cfg['timeouts']['short_pause'])
                click_text(rp, r_name, driver)
                if not apply_region_and_wait(driver, config):
                    log.warning(f"    '{l_name} {r_name}' 지역 이동 실패"); continue
                
                links_in_region = collect_links(driver, fetcher_cfg['base_url'], config)
                log.info(f"    '{r_name}'에서 {len(links_in_region)}개 링크 발견")
                all_links.update(links_in_region)
            except Exception as e:
                log.error(f"    '{r_name}' 처리 중 오류: {e}"); continue
            break
        break
    return list(all_links)

# ==============================================================================
# 3. 상세 페이지 HTML 저장 로직 (두 번째 코드의 안정적인 로직 '그대로' 복원)
# ==============================================================================

# ▼▼▼ [핵심 복원] 지적하신 대로, 두 번째 코드의 리뷰 관련 함수들을 단 한 줄도 바꾸지 않고 그대로 가져왔습니다. ▼▼▼
def ensure_review_tab_active(driver, config):
    wait = WebDriverWait(driver, config['fetcher']['timeouts']['wait'])
    tab = wait.until(EC.presence_of_element_located(
        (By.XPATH, config['parser']['review_tab_xpath'])
    ))
    selected = (tab.get_attribute("aria-selected") or "").lower() == "true"
    active   = (tab.get_attribute("data-state") or "").lower() == "active"
    if not (selected or active):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", tab)
        time.sleep(config['fetcher']['timeouts']['short_pause'])
        try: tab.click()
        except ElementClickInterceptedException:
            driver.execute_script("arguments[0].click();", tab)
        time.sleep(config['fetcher']['timeouts']['pause'])
    return tab

def get_reviews_panel(driver, config):
    wait = WebDriverWait(driver, config['fetcher']['timeouts']['wait'])
    tab = ensure_review_tab_active(driver, config)
    pid = tab.get_attribute("aria-controls")
    if not pid: return None
    try:
        return driver.find_element(By.ID, pid)
    except NoSuchElementException:
        return None

def click_all_filter_if_exists(config, panel, driver):
    try:
        all_btn = panel.find_element(By.XPATH, ".//button[normalize-space()='전체']")
        try: all_btn.click()
        except ElementClickInterceptedException:
            driver.execute_script("arguments[0].click();", all_btn)
        time.sleep(config['fetcher']['timeouts']['short_pause'])
    except NoSuchElementException:
        pass

def count_cards(panel, config):
    try:
        return len(panel.find_elements(By.CSS_SELECTOR, config['parser']['review_card_css']))
    except Exception:
        return 0

def focus_and_keyscroll_panel(driver, panel, config):
    fetcher_cfg = config['fetcher']
    actions = ActionChains(driver)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", panel)
    try: panel.click()
    except Exception: pass

    last_cnt = -1
    stable = 0
    steps = 0
    while steps < fetcher_cfg['timeouts']['max_scroll_steps']:
        steps += 1
        actions.send_keys(Keys.PAGE_DOWN).pause(0.05).send_keys(Keys.PAGE_DOWN).perform()
        time.sleep(fetcher_cfg['timeouts']['scroll_pause'])
        new_cnt = count_cards(panel, config)
        if new_cnt == last_cnt:
            stable += 1
            if steps % 15 == 0:
                actions.send_keys(Keys.END).perform()
                time.sleep(fetcher_cfg['timeouts']['scroll_pause'])
                new_cnt = count_cards(panel, config)
                if new_cnt != last_cnt:
                    stable = 0; last_cnt = new_cnt; continue
            if stable >= fetcher_cfg['timeouts']['scroll_patience']:
                break
        else:
            stable = 0; last_cnt = new_cnt
    log.info(f"리뷰 스크롤 완료: {last_cnt}개 로드됨")
    return last_cnt
# ▲▲▲ [핵심 복원] ▲▲▲
import json
# ==============================================================================
# 4. Airflow PythonOperator가 호출할 메인 실행 함수
# ==============================================================================
def run_fetcher(config_path: str, ds_nodash: str):
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    s3_cfg = config['s3']
    s3_hook = S3Hook(aws_conn_id=s3_cfg['aws_conn_id'])
    s3_prefix = f"{s3_cfg['fetcher_prefix']}/{ds_nodash}"

    log.info("=== 1단계: 모든 정비소 URL 수집 시작 ===")
    driver = build_driver(config)
    try:
        all_shop_urls = collect_all_shop_links(driver, config)
        log.info(f"총 {len(all_shop_urls)}개의 고유 URL을 수집했습니다.")
    finally:
        driver.quit()

    if not all_shop_urls:
        log.warning("수집된 URL이 없습니다. 작업을 종료합니다.")
        return s3_prefix

    log.info("=== 2단계: 각 URL 방문 및 최종 HTML 저장 시작 ===")
    driver = build_driver(config)
    try:
        # 테스트 고정
        all_shop_urls = ["https://repair.cardoc.co.kr/shops/0152d4a0-0bd1-7000-8000-0000000000e5"]

        for i, url in enumerate(all_shop_urls, 1):
            shop_id = extract_shop_id(url)
            log.info(f"({i}/{len(all_shop_urls)}) 처리 중: {url}")

            try:
                driver.get(url)

                # 2-1) 후기 탭 건드리기 전에, 기본 탭(업체 정보)에서 '주소' 먼저 추출
                addr_text = extract_address_text(
                    driver, wait_sec=config['fetcher']['timeouts']['wait']
                )
                addr_slug = slugify_for_s3(addr_text)

                # 2-2) 이제 후기 탭으로 전환/스크롤(기존 로직 유지)
                panel = get_reviews_panel(driver, config)
                if panel:
                    click_all_filter_if_exists(config, panel, driver)
                    focus_and_keyscroll_panel(driver, panel, config)
                else:
                    log.info("    리뷰 패널이 없어 스크롤을 건너뜁니다.")

                # 2-3) HTML 스냅샷 저장 (주소 기반 파일명)
                # 주소가 있으면: {shop_id}__{addr}.html
                # 없으면: {shop_id}.html
                if addr_slug:
                    file_name = f"{shop_id}__{addr_slug}.html"
                else:
                    file_name = f"{shop_id}.html"

                s3_key = f"{s3_prefix}/{file_name}"
                html_content = driver.page_source

                s3_hook.load_string(
                    string_data=html_content,
                    key=s3_key,
                    bucket_name=s3_cfg['source_bucket'],
                    replace=True
                )
                log.info(f"    -> S3 저장 완료: s3://{s3_cfg['source_bucket']}/{s3_key}")

                # (선택) 주소 매핑 sidecar JSON도 함께 저장하면 Parser가 합치기 쉬움
                # ex) s3://.../{shop_id}__meta.json
                meta_key = f"{s3_prefix}/{shop_id}__meta.json"
                meta_json = {
                    "shop_id": shop_id,
                    "url": url,
                    "address": addr_text
                }
                s3_hook.load_string(
                    string_data=json.dumps(meta_json, ensure_ascii=False),
                    key=meta_key,
                    bucket_name=s3_cfg['source_bucket'],
                    replace=True
                )
                log.info(f"    -> 메타 저장 완료: s3://{s3_cfg['source_bucket']}/{meta_key}")

                time.sleep(config['fetcher']['timeouts']['shop_gap'])

            except Exception as e:
                log.error(f"URL 처리 실패: {url}", exc_info=True)

    finally:
        driver.quit()

    log.info("모든 HTML 파일 저장을 완료했습니다.")
    return s3_prefix
