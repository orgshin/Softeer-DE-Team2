from math import log1p
from typing import Optional, Dict, Any
from schemas.domain import EstimateDetail
from utils.config import CFG

def clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))

def to_label(price_score: int, quality_score: int) -> str:
    if price_score >= 60 and quality_score >= 60:
        return "추천"
    if price_score < 40 or quality_score < 40:
        return "위험"
    return "재견적"

def score_quality(shop: Optional[Dict[str, Any]]) -> int:
    if not shop:
        return 55
    grade = str(shop.get("grade", "2"))
    grade_norm = {"1": 1.0, "2": 0.7, "3": 0.4}.get(grade, 0.6)
    pos_ratio = float(shop.get("positive_ratio", 0.5))
    rcnt = int(shop.get("review_count", 0))
    review_norm = clamp01(log1p(min(rcnt, 1000)) / log1p(1000))
    q = (
        grade_norm * CFG.shop_grade_weight
        + pos_ratio * CFG.shop_positive_ratio_weight
        + review_norm * CFG.shop_review_weight
    )
    return round(clamp01(q) * 100)

def score_price(detail: EstimateDetail, total: float) -> int:
    fee_weighted_gap = 0.0
    fee_weight_total = 0.0
    for it in detail.fee:
        if it.market_price and it.market_price > 0:
            w = it.value
            gap_ratio = abs((it.value - it.market_price) / it.market_price)
            fee_weighted_gap += w * gap_ratio
            fee_weight_total += w
    fee_penalty = (fee_weighted_gap / fee_weight_total) if fee_weight_total > 0 else 0.0
    fee_component = clamp01(max(0.0, 1.0 - (fee_penalty / 0.35)))

    parts_weighted_gap = 0.0
    parts_weight_total = 0.0
    for it in detail.parts:
        if it.market_price and it.market_price > 0:
            w = it.value
            gap_ratio = abs((it.value - it.market_price) / it.market_price)
            parts_weighted_gap += w * gap_ratio
            parts_weight_total += w
    if parts_weight_total > 0:
        parts_penalty = parts_weighted_gap / parts_weight_total
        parts_component = clamp01(max(0.0, 1.0 - (parts_penalty / 0.35)))
    else:
        parts_component = 0.5

    price_norm = CFG.fee_weight * fee_component + CFG.parts_weight * parts_component
    return round(clamp01(price_norm) * 100)

def one_liner(quotation_id: int, price_score: int, quality_score: int, label: str) -> str:
    return f"견적서 {quotation_id}는 가격 {price_score}점·품질 {quality_score}점으로 '{label}'입니다."

def winner_line(qid: int) -> str:
    return f"견적서 {qid}가 다른 견적서 대비 가격·품질 모두 합리적이므로 추천합니다."
