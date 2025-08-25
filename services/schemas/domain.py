from typing import Optional, List
from pydantic import BaseModel
from utils.config import CFG

class Tag(BaseModel):
    action: str
    reason: str

class ItemAnalysis(BaseModel):
    name: str
    value: float
    average: Optional[float] = None
    market_price: Optional[float] = None
    gap_price: Optional[float] = None
    gap_pct: Optional[float] = None
    tag: Tag

    @classmethod
    def no_ref(cls, name: str, value: float) -> "ItemAnalysis":
        return cls(
            name=name, value=value,
            average=None, market_price=None, gap_price=None, gap_pct=None,
            tag=Tag(action="proceed", reason="no_reference")
        )

    @classmethod
    def with_ref(cls, name: str, value: float, market: float, cfg=CFG) -> "ItemAnalysis":
        gap = value - market
        gap_pct = (gap / market) if market > 0 else 0.0

        if abs(gap_pct) <= cfg.market_tolerance_pct:
            tag = Tag(action="proceed", reason="within_market")
        elif gap_pct > cfg.hard_over_pct:
            tag = Tag(action="reject", reason="over_market")
        elif gap_pct > 0:
            tag = Tag(action="requote", reason="over_market")
        else:
            tag = Tag(action="proceed", reason="under_market")

        return cls(
            name=name, value=value,
            average=market, market_price=market,
            gap_price=gap, gap_pct=round(gap_pct * 100, 1),
            tag=tag
        )

class FeeItem(BaseModel):
    name: str
    value: float

class PartItem(BaseModel):
    code: Optional[str] = None
    name: str
    value: float

class EstimateDetail(BaseModel):
    parts: List[ItemAnalysis]
    fee: List[ItemAnalysis]
