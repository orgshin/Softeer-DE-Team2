# mobis_parser.py
import os
import re
import yaml
import logging
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession

# ================= Spark 세션 =================
def build_spark(app_name="MobisHTMLParser"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .master("local[*]")
        .getOrCreate()
    )
    return spark

# ================= YAML 설정 로드 =================
def load_config(yaml_path="configs/parts/mobis.yaml"):
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# ================= HTML 파싱 =================
def clean(text: str) -> str:
    if text is None:
        return ""
    return re.sub(r"\s+", " ", text.strip())

def parse_html_file(file_path, meta, selectors):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            soup = BeautifulSoup(f, "lxml")

        rows = []
        table_wrap = soup.select_one(selectors["table_wrap"])
        if not table_wrap:
            logging.warning(f"[WARN] 테이블 영역 없음: {file_path}")
            return rows

        row_groups = table_wrap.select(selectors["row_group"])
        if not row_groups:
            logging.warning(f"[WARN] row 그룹 없음: {file_path}")
            return rows

        for ul in row_groups:
            if "table-h" in ul.get("class", []):
                continue
            li_rows = ul.select(selectors["row"])
            if not li_rows:
                logging.warning(f"[WARN] 데이터 row 없음: {file_path}")
                continue
            for li in li_rows:
                vals = li.select(selectors["cell"])
                if len(vals) < 4:
                    logging.warning(f"[WARN] row 셀 개수 부족 ({len(vals)}): {file_path}")
                    continue

                part_no = vals[0].get_text(strip=True) if vals[0] else ""
                ko = vals[1].get_text(strip=True) if vals[1] else ""
                en = vals[2].get_text(strip=True) if vals[2] else ""
                price = vals[3].get_text(strip=True) if vals[3] else ""

                # 필수 컬럼 체크
                if not part_no or not ko or not price:
                    logging.warning(f"[WARN] 필수값 누락: part_no='{part_no}', ko='{ko}', price='{price}' ({file_path})")
                    continue

                rows.append({
                    "제조사": meta["maker"],
                    "차량구분": meta["vtype"],
                    "모델": meta["model"],
                    "부품번호": clean(part_no),
                    "한글부품명": clean(ko),
                    "영문부품명": clean(en),
                    "가격": clean(price)
                })
        return rows
    except Exception as e:
        logging.error(f"[ERROR] {file_path} 파싱 실패: {e}")
        return []

# ================= 메인 =================
def run_parser(yaml_path="config/parts/mobis.yaml"):
    config = load_config(yaml_path)
    html_dir = config.get("html_paths", "data/raw/parts/mobis")
    output_file = config.get("output_file", "data/raw/parts/mobis_parts.parquet")

    # ================= 셀렉터 상수 =================
    table_wrap_selector = "#table-wrap div.table-list.sp-table"
    row_group_selector = "ul.table-d"
    row_selector = "li[role='row']"
    cell_selector = "span.t-td[role='cell']"

    # ====== 로그 설정 ======
    log_file = config.get("log_filename", "data/log/parts/mobis.log")
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

    spark = build_spark()
    all_rows = []

    files = [os.path.join(html_dir, f) for f in os.listdir(html_dir) if f.endswith(".html")]

    for file_path in files:
        fname = os.path.basename(file_path)
        try:
            maker, vtype, model, keyword, _ = fname.replace(".html", "").split("__", 4)
        except ValueError:
            maker, vtype, model, keyword = ("?", "?", "?", "?")

        # 모델명 공백 복원
        model = model.replace("_", " ")
        meta = {"maker": maker, "vtype": vtype, "model": model, "keyword": keyword}

        all_rows.extend(parse_html_file(
            file_path,
            meta,
            selectors={
                "table_wrap": table_wrap_selector,
                "row_group": row_group_selector,
                "row": row_selector,
                "cell": cell_selector
            }
        ))

    if not all_rows:
        logging.error("ERROR] 추출된 데이터가 없습니다.")
        return None

    df = spark.createDataFrame(all_rows)
    df.write.mode("overwrite").parquet(output_file)
    logging.info(f"[INFO] 결과 저장 완료: {output_file}")
    return df

if __name__ == "__main__":
    CONFIG_PATH = "config/parts/mobis.yaml"
    run_parser(CONFIG_PATH)