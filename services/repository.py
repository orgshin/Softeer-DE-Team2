# repository.py
from __future__ import annotations
from typing import Dict, Any, Optional, List
from sqlalchemy.orm import Session
from models import Part, OfficialFee, Quote, MobisPart
import json

class PartsRepository:
    """
    부품/공임 데이터 접근 레포지토리 (PostgreSQL 기반)
    """
    def __init__(self, db: Session):
        self.db = db

    def get_part_avg_price(self, vendor_code: str) -> Optional[float]:
        """
        'parts_avg_price' 테이블에서 부품 시장 평균가 조회
        """
        part = self.db.query(Part).filter(Part.vendor_code == vendor_code).first()
        return part.avg_price if part else None

    def get_mobis_part_price(self, vendor_code: str) -> Optional[int]:
        """
        'mobis_parts' 테이블에서 부품 공식 가격 조회
        """
        mobis_part = self.db.query(MobisPart).filter(MobisPart.부품번호 == vendor_code).first()
        return mobis_part.가격 if mobis_part else None

    # ✨ [수정] LLM의 문맥 이해를 돕기 위해 official_fee 테이블의 모든 텍스트 정보를 반환하도록 변경
    def get_all_fees(self) -> List[Dict[str, Any]]:
        """
        'official_fee' 테이블에서 모든 공임 정보를 상세히 조회
        """
        fees = self.db.query(OfficialFee).all()
        return [
            {
                "label": fee.label, 
                "value": fee.fee_amount, 
                "category": fee.category_name,
                "section": fee.section_title,
                "description": fee.work_type
            }
            for fee in fees
        ]

class QuoteRepository:
    """
    견적 분석 결과 저장소 (PostgreSQL 기반)
    """
    def __init__(self, db: Session):
        self.db = db

    def save(self, quote_id: str, result: Dict[str, Any]) -> None:
        db_quote = Quote(id=quote_id, analysis_result=result)
        self.db.add(db_quote)
        self.db.commit()
        self.db.refresh(db_quote)

    def get(self, quote_id: str) -> Optional[Dict[str, Any]]:
        db_quote = self.db.query(Quote).filter(Quote.id == quote_id).first()
        return db_quote.analysis_result if db_quote else None

    def get_many(self, ids: list[str]) -> list[Dict[str, Any]]:
        quotes = self.db.query(Quote).filter(Quote.id.in_(ids)).all()
        return [{"id": q.id, **q.analysis_result} for q in quotes]
