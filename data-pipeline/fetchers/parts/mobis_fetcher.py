# mobis_crawl_fetcher.py
import logging
import random
import time
import os
import yaml
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

# ================== 유틸 ==================
def jitter(a=0.8, b=1.6):
    time.sleep(random.uniform(a, b))

def jsclick(driver, el):
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
    driver.execute_script("arguments[0].click();", el)

def safe_filename(s: str) -> str:
    return re.sub(r"[^\w\-_.:]", "_", s)

def accept_any_alert(driver):
    try:
        driver.switch_to.alert.accept()
        time.sleep(0.15)
        return True
    except Exception:
        return False

def wait_idle(driver, timeout=10):
    """페이지 AJAX 완료 대기"""
    end = time.time() + timeout
    while time.time() < end:
        if accept_any_alert(driver):
            continue
        try:
            st = driver.find_element(By.ID, "isRun").get_attribute("value").upper()
            if st == "N":
                return True
        except Exception:
            return True
        time.sleep(0.1)
    return False

def ensure_selection(driver, maker, vtype, wait):
    make_id = "make1" if maker == "현대" else "make2"
    use_id = "use1" if vtype == "승용" else "use2"

    if not driver.find_element(By.ID, make_id).is_selected():
        lbl = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"label[for='{make_id}']")))
        jsclick(driver, lbl); jitter()
    if not driver.find_element(By.ID, use_id).is_selected():
        lbl = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, f"label[for='{use_id}']")))
        jsclick(driver, lbl); jitter()
    time.sleep(0.4)

# ================== HTML Fetcher ==================
class HTMLFetcher:
    def __init__(self, config_path: str):
        with open(config_path, "r", encoding="utf-8") as f:
            self.cfg = yaml.safe_load(f)
        
        self.base_url = self.cfg["base_url"]
        self.manufacturers = self.cfg["manufacturers"]
        self.vehicle_types = self.cfg["vehicle_types"]
        self.part_keywords = self.cfg["part_keywords"]
        self.timeouts = self.cfg.get("timeouts", {})
        self.cooldowns = self.cfg.get("cooldowns", {})
        self.chrome_opts_cfg = self.cfg.get("chrome_options", {})

        self.driver = None
        self.wait = None
        self.html_dir = self.cfg.get("html_paths", "data/raw/parts/mobis")
        os.makedirs(self.html_dir, exist_ok=True)

        # ====== 로그 설정 ======
        log_file = self.cfg.get("log_filename", "data/log/parts/mobis.log")
        os.makedirs(os.path.dirname(log_file) or ".", exist_ok=True)
        if not os.path.exists(log_file):
            open(log_file, 'w').close()

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(levelname)s] %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
                logging.StreamHandler()
            ]
        )
        logging.info(f"[INFO] 로그 파일: {log_file}")

    def build_driver(self, timeout: int = 15):
        opts = webdriver.ChromeOptions()
        ws = self.chrome_opts_cfg.get("window_size", [1400, 960])
        opts.add_argument(f"--window-size={ws[0]},{ws[1]}")

        if self.chrome_opts_cfg.get("headless", False):
            opts.add_argument("--headless=new")

        user_agents = self.chrome_opts_cfg.get("user_agents", [])
        if user_agents:
            ua = random.choice(user_agents)
            opts.add_argument(f"user-agent={ua}")

        if self.chrome_opts_cfg.get("disable_automation_flags", True):
            opts.add_argument("--disable-blink-features=AutomationControlled")
            opts.add_experimental_option("excludeSwitches", ["enable-automation"])
            opts.add_experimental_option("useAutomationExtension", False)

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=opts)
        self.driver.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {"source": "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"}
        )

        self.wait = WebDriverWait(self.driver, timeout)
        logging.info(f"[INFO] 크롬 드라이버 실행, UA 적용됨: {user_agents}")
        return self.driver

    def get_models(self, maker, vtype):
        if not self.driver:
           self.build_driver()

        self.driver.get(self.base_url)
        ensure_selection(self.driver, maker, vtype, self.wait)

        sel_el = self.wait.until(EC.presence_of_element_located((By.ID, "model")))
        sel = Select(sel_el)
        WebDriverWait(self.driver, 10).until(
            lambda d: len([o for o in sel.options if o.text.strip() not in ("전체", "선택", "")]) >= 1
        )
        models = [o.text.strip() for o in sel.options if o.text.strip() not in ("전체", "선택", "")]
        logging.info(f"[INFO] 모델 목록: {models}")
        return models

    def fetch_html_for_model(self, maker, vtype, model_name):
        if not self.driver:
            self.build_driver()
        self.driver.get(self.base_url)
        ensure_selection(self.driver, maker, vtype, self.wait)

        sel_el = self.wait.until(EC.element_to_be_clickable((By.ID, "model")))
        sel = Select(sel_el)
        sel.select_by_visible_text(model_name)
        time.sleep(1)

        for kw in self.part_keywords:
            kw_input = self.wait.until(EC.visibility_of_element_located((By.ID, "searchNm")))
            kw_input.clear()
            kw_input.send_keys(kw)

            btn = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".btn-wrap > .btn.btn-red")))
            jsclick(self.driver, btn)
            wait_idle(self.driver, timeout=self.timeouts.get("idle_timeout", 25))

            # ---------------- 총 부품 수 확인 ----------------
            try:
                total_rows_el = self.driver.find_element(By.XPATH, '//*[@id="table-wrap"]/div[1]/span')
                total_rows = int(re.sub(r"[^\d]", "", total_rows_el.text))
                group_size = 10
                total_pages = (total_rows + group_size - 1) // group_size
            except Exception:
                total_pages = 1
                logging.warning(f"[WARN] 총 부품 수 확인 실패, 기본 1페이지로 처리: {kw}")

            page = 1
            while page <= total_pages:
                html = self.driver.page_source
                filename = f"{safe_filename(maker)}__{safe_filename(vtype)}__{safe_filename(model_name)}__{safe_filename(kw)}__page{page}.html"
                filepath = os.path.join(self.html_dir, filename)
                html = self.driver.page_source

                # ----------------- 데이터 유효성 검사 -----------------
                soup = BeautifulSoup(html, "lxml")

                # table-wrap 안에 table이 있는지 확인
                table_wrap = soup.select_one("#table-wrap div.table-list.sp-table")
                if not table_wrap:
                    logging.warning(f"[WARN] 데이터 테이블 없음, 저장 건너뜀: {filename}")
                    page += 1
                    continue  # table 없으면 저장하지 않고 다음 페이지로 넘어감

                # 실제 row가 있는지 확인
                rows = table_wrap.select("ul.table-d li[role='row']")
                if len(rows) == 0:
                    logging.warning(f"[WARN] 데이터 행 없음, 저장 건너뜀: {filename}")
                    page += 1
                    continue  # row가 없으면 저장하지 않음
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(html)
                logging.info(f"[SAVE] {filepath}")

                if page % group_size == 0 and page < total_pages:
                    try:
                        next_group_btn = self.driver.find_elements(By.XPATH, '//*[@id="table-wrap"]//ul/li[contains(@class,"arrow")]/a')[2]
                        jsclick(self.driver, next_group_btn)
                        wait_idle(self.driver, timeout=self.timeouts.get("idle_timeout", 25))
                        jitter(*self.cooldowns.get("page_pause", [1.2, 1.8]))
                    except Exception:
                        logging.warning(f"[WARN] 다음 페이지 그룹 이동 실패: {kw}")
                else:
                    # ---------------- 일반 페이지 이동 ----------------
                    try:
                        indi = self.driver.find_element(By.CSS_SELECTOR, "#table-wrap ul.indicator")
                        cur_page = indi.find_element(By.CSS_SELECTOR, "li.on a").text.strip()
                        pages = [a for a in indi.find_elements(By.TAG_NAME, "a") if a.text.strip().isdigit()]
                        next_page = None
                        for a in pages:
                            if int(a.text.strip()) > int(cur_page):
                                next_page = a
                                break
                        if next_page:
                            jsclick(self.driver, next_page)
                            wait_idle(self.driver, timeout=self.timeouts.get("idle_timeout", 25))
                            jitter(*self.cooldowns.get("page_pause", [1.2, 1.8]))
                    except Exception:
                        logging.info(f"[INFO] 마지막 페이지 도달 또는 페이지 이동 불가: {kw}")
                page += 1

            jitter(*self.cooldowns.get("kw_cooldown", [2,4]))

    def close(self):
        if self.driver:
            logging.info("[INFO] 드라이버 종료")
            self.driver.quit()


if __name__ == "__main__":
    CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "config", "parts","mobis.yaml")
    fetcher = HTMLFetcher(config_path=CONFIG_PATH)

    for maker in fetcher.manufacturers:
        for vtype in fetcher.vehicle_types:
            models = fetcher.get_models(maker, vtype)
            for model_idx, model_name in enumerate(models):
                logging.info(f"[INFO] {safe_filename(maker)} {safe_filename(vtype)} {safe_filename(model_name)}(model_idx: {model_idx}) 시작")
                fetcher.fetch_html_for_model(maker, vtype, model_name)

    fetcher.close()
