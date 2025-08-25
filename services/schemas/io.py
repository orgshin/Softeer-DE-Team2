from typing import List, Optional, Dict
from pydantic import BaseModel, Field

class RepairShopIn(BaseModel):
    name: str
    address: Optional[str] = None

class PartItem(BaseModel):
    code: Optional[str] = None
    name: str
    value: float

class FeeItem(BaseModel):
    name: str
    value: float

class EstimateIn(BaseModel):
    repair_shop: Optional[RepairShopIn] = None
    car: Dict[str, str] = Field(..., example={"model": "아반떼 7"})
    parts: List[PartItem] = []
    fee: List[FeeItem] = []
    total: Optional[float] = None

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

class EstimateDetail(BaseModel):
    parts: List[ItemAnalysis]
    fee: List[ItemAnalysis]

class EstimateOut(BaseModel):
    quotation_id: int
    regist_date: str
    total: float
    detail: EstimateDetail
    price_score: int
    quality_score: int
    grade: str
    one_line_review: str

class WinnerBlock(BaseModel):
    id: int
    price_score: int
    quality_score: int
    grade: str
    regist_date: str
    one_line_review: str

class CompareOut(BaseModel):
    winner: WinnerBlock
    quotation: List[WinnerBlock]
