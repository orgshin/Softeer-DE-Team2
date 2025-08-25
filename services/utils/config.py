from dataclasses import dataclass

@dataclass
class ScoringConfig:
    fee_weight: float = 0.75
    parts_weight: float = 0.25
    market_tolerance_pct: float = 0.12
    hard_over_pct: float = 0.35
    shop_grade_weight: float = 0.45
    shop_positive_ratio_weight: float = 0.4
    shop_review_weight: float = 0.15

CFG = ScoringConfig()
