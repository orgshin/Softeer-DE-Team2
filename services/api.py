# api.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from service import RepairService, get_service
from schema import *
from database import get_db

router = APIRouter(tags=["Car Repair Quote"])

@router.post("/estimate", response_model=FullAnalysisResponse, summary="Analyze a Car Repair Quote")
def analyze_quote(
    repair_input: RepairInput,
    svc: RepairService = Depends(get_service),
):
    """
    입력된 단일 견적서에 대해 항목별·총액 평가 결과를 반환합니다.
    """
    try:
        result = svc.analyze_and_save_quote(repair_input.model_dump())
        return result
    except HTTPException:
        raise
    except Exception as e:
        # 실제 운영 환경에서는 더 구체적인 예외 처리가 필요합니다.
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/compare", summary="Compare Multiple Analyzed Quotes")
def compare_quotes(
    compare_input: CompareInput,
    svc: RepairService = Depends(get_service),
):
    """
    여러 개의 견적서를 입력받아 가격·품질 점수를 산정하고, 최종적으로 추천 견적서를 반환합니다.
    (현재 DB 연동 로직은 구현되지 않음)
    """
    try:
        return svc.compare_quotes(compare_input.quote_ids)
    except NotImplementedError:
        raise HTTPException(status_code=501, detail="This feature is not yet implemented.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

