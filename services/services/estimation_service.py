from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session

from repositories.official_fee_repository import OfficialFeeRepository
from repositories.repairshop_repository import RepairshopRepository
from repositories.parts_repository import PartsRepository

from schemas.io import EstimateIn, CompareOut, EstimateOut, EstimateDetail as IOEstimateDetail, WinnerBlock
# ⬇️ domain 스키마는 내부 계산용으로 사용
from schemas.domain import FeeItem, PartItem, EstimateDetail as DomainEstimateDetail, ItemAnalysis

from utils.config import CFG
from utils.scoring import score_quality, score_price, to_label, winner_line, one_liner, clamp01
from utils.text import parse_vehicle_tokens, best_fee_match

class EvaluateResult:
    def __init__(self, total: float, detail: DomainEstimateDetail, price_score: int, quality_score: int, grade: str):
        self.total = total
        self.detail = detail
        self.price_score = price_score
        self.quality_score = quality_score
        self.grade = grade
        
class EstimationService:
    def __init__(self, db: Optional[Session]):
        self.db = db

    # ---- 공식 공임 레퍼런스 수집/가공 ----
    def _fetch_official_fee_refs(self, car_model: Optional[str]) -> List[Dict[str, Any]]:
        repo = OfficialFeeRepository(self.db)
        rows = repo.fetch_rows()

        acc_sum: Dict[str, float] = {}
        acc_cnt: Dict[str, int] = {}

        def _push(name: str, price: float):
            acc_sum[name] = acc_sum.get(name, 0.0) + float(price)
            acc_cnt[name] = acc_cnt.get(name, 0) + 1

        car_key = (car_model or "").lower().strip()
        if car_key:
            for r in rows:
                task = r["task_name"]
                price = r["price"]
                tokens = parse_vehicle_tokens(r["vehicle"])
                tokens_norm = [t.lower() for t in tokens]
                if any(car_key in t for t in tokens_norm):
                    _push(task, price)

        if not acc_cnt:
            for r in rows:
                _push(r["task_name"], r["price"])

        out = []
        for name, s in acc_sum.items():
            out.append({"task_name": name, "avg_price": s / acc_cnt[name]})
        return out

    # ---- 아이템 분석 ----
    def _analyze_fee_items(self, car_model: Optional[str], fee_items: List[FeeItem]) -> List[ItemAnalysis]:
        refs = self._fetch_official_fee_refs(car_model)
        out: List[ItemAnalysis] = []
        for it in fee_items:
            ref = best_fee_match(it.name, refs)
            if not ref:
                out.append(ItemAnalysis.no_ref(it.name, it.value))
                continue
            market = float(ref["avg_price"])
            out.append(ItemAnalysis.with_ref(it.name, it.value, market, CFG))
        return out

    def _analyze_part_items(self, parts: List[PartItem]) -> List[ItemAnalysis]:
        repo = PartsRepository(self.db)
        code_list = [p.code for p in parts if p.code]
        refs = repo.fetch_refs_by_code(code_list) if code_list else {}

        out: List[ItemAnalysis] = []
        for p in parts:
            ref_price = refs.get(p.code) if p.code else None
            if ref_price is None:
                out.append(ItemAnalysis.no_ref(p.name, p.value))
                continue
            out.append(ItemAnalysis.with_ref(p.name, p.value, float(ref_price), CFG))
        return out

    # ---- 퍼사드 (평가) ----
    def evaluate(self, payload: EstimateIn) -> EvaluateResult:
        car_model = payload.car.get("model")

        fee_detail = self._analyze_fee_items(car_model, [FeeItem(**f.model_dump()) for f in payload.fee])
        part_detail = self._analyze_part_items([PartItem(**p.model_dump()) for p in payload.parts])
        detail = DomainEstimateDetail(parts=part_detail, fee=fee_detail)

        total = payload.total
        if total is None:
            total = sum(i.value for i in payload.parts) + sum(i.value for i in payload.fee)

        # 품질(정비소)
        quality = 55
        if payload.repair_shop:
            shop = RepairshopRepository(self.db).fetch_repairshop(payload.repair_shop.name, payload.repair_shop.address)
            quality = score_quality(shop)

        price = score_price(detail, total)
        label = to_label(price, quality)
        return EvaluateResult(total, detail, price, quality, label)

    # ---- 출력 만들기 ----
    def build_estimate_out(self, quotation_id: int, total: float,
                           detail: DomainEstimateDetail, price_score: int,
                           quality_score: int, grade: str) -> EstimateOut:
        io_detail = IOEstimateDetail.model_validate(detail.model_dump())
        return EstimateOut(
            quotation_id=quotation_id,
            regist_date=datetime.now().strftime("%Y-%m-%d %H:%M"),
            total=total,
            detail=io_detail,
            price_score=price_score,
            quality_score=quality_score,
            grade=grade,
            one_line_review=one_liner(quotation_id, price_score, quality_score, grade),
        )

    # ✨ DynamoDB 저장용 직렬화 딕셔너리
    @staticmethod
    def to_result_dict(out: EstimateOut) -> Dict[str, Any]:
        """
        EstimateOut → dict (DynamoDB 저장용)
        """
        return out.model_dump()
    def build_compare_out(self, estimates: List[EstimateOut]) -> CompareOut:
        def rank_key(x: EstimateOut):
            return 0.55 * x.price_score + 0.45 * x.quality_score

        all_items = sorted(estimates, key=rank_key, reverse=True)

        top = all_items[0]
        winner = WinnerBlock(
            id=top.quotation_id,
            price_score=top.price_score,
            quality_score=top.quality_score,
            grade=top.grade,
            regist_date=top.regist_date,
            one_line_review=winner_line(top.quotation_id)
        )
        others = [WinnerBlock(
            id=x.quotation_id,
            price_score=x.price_score,
            quality_score=x.quality_score,
            grade=x.grade,
            regist_date=x.regist_date,
            one_line_review=x.one_line_review
        ) for x in all_items]

        return CompareOut(winner=winner, quotation=others)