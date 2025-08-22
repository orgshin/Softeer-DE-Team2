# schema.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any

# --- Request Schemas ---

class CarInfo(BaseModel):
    model: str = Field(..., description="The model name of the car.")

class PartItem(BaseModel):
    code: str = Field(..., description="The unique identifier code for the part.")
    name: str = Field(..., description="The name of the part.")
    value: int = Field(..., description="The price of the part in the quote.")

class FeeItem(BaseModel):
    name: str = Field(..., description="The name of the labor/fee item.")
    value: int = Field(..., description="The price of the labor/fee in the quote.")

class RepairInput(BaseModel):
    car: CarInfo
    parts: List[PartItem] = Field(default_factory=list)
    fee: List[FeeItem] = Field(default_factory=list)
    total: Optional[int] = Field(None)

class CompareInput(BaseModel):
    quote_ids: List[str] = Field(..., description="A list of quote IDs to be compared.")


# --- Response Schemas ---

class Tag(BaseModel):
    action: str
    reason: str

class AnalyzedPart(BaseModel):
    name: str
    value: int
    average: Optional[float]
    market_price: Optional[float] # API 문서 호환성을 위해 추가 (average와 동일 값 사용)
    gap_price: Optional[float]
    gap_pct: Optional[float]
    tag: Tag
    one_line_review: str

class AnalyzedFee(BaseModel):
    name: str
    value: int
    average: Optional[int]
    gap_price: Optional[int]
    gap_pct: Optional[float]
    tag: Tag
    one_line_review: str

class AnalysisDetail(BaseModel):
    parts: List[AnalyzedPart]
    fee: List[AnalyzedFee]

class AnalysisResponse(BaseModel):
    total: int
    one_line_review: str
    detail: AnalysisDetail

class FullAnalysisResponse(BaseModel):
    quote_id: str
    analysis: AnalysisResponse
