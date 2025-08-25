# apis/estimate.py
from typing import List
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from database import get_db
from schemas.io import EstimateIn, CompareOut, EstimateOut, WinnerBlock
from services.estimation_service import EstimationService
from repositories.dynamodb_repository import DynamoDBRepository

router = APIRouter()

@router.post("/estimate", response_model=CompareOut)
def estimate(payload: EstimateIn, db: Session = Depends(get_db)):
    service = EstimationService(db)
    repo = DynamoDBRepository()

    # 1) 평가
    res = service.evaluate(payload)

    # 2) quote_id 생성 (응답은 int로 유지 / 저장은 str로)
    #    - 운영에서 일관성 원하면 uuid 기반으로 바꾸고 스키마도 str로 맞추면 됨.
    quotation_id = int(uuid4().int % 10**9)  # 9자리 임의 정수
    quote_id_str = str(quotation_id)

    # 3) EstimateOut 생성 (응답 스키마)
    out = service.build_estimate_out(
        quotation_id=quotation_id,
        total=res.total,
        detail=res.detail,
        price_score=res.price_score,
        quality_score=res.quality_score,
        grade=res.grade,
    )

    # 4) DynamoDB 저장 (입력/출력 모두)
    try:
        repo.save_user_input(quote_id_str, payload.model_dump())
        repo.save_analysis_result(quote_id_str, service.to_result_dict(out))
    except Exception:
        # 저장 실패해도 응답은 보냄 (로그는 리포지토리 내부에서 찍음)
        pass

    # 5) 전체 비교 응답 구성: DynamoDB에서 전부 읽어와 정렬
    #    - 비용 절약 위해선 목록 API/인덱스 설계 추천 (예: GSI로 created_at 정렬)
    items = repo.batch_get_analysis_results([quote_id_str])  # 최신 1건만은 빈약하니…
    # 현실적으로는 "최근 N건" 조회가 필요 → GSI/Scan 등 추가 설계 권장
    # 여기서는 방금 저장한 1건만으로 CompareOut 구성 (데모)
    if not items:
        # 방금 저장한 out을 그대로 사용
        estimates = [out]
    else:
        # Dynamo 아이템 → Pydantic 변환
        estimates: List[EstimateOut] = []
        for it in items:
            # Dynamo 값이 문자열일 수 있으므로 보정
            try:
                it["quotation_id"] = int(it.get("quotation_id", quotation_id))
            except:
                it["quotation_id"] = quotation_id
            estimates.append(EstimateOut.model_validate(it))

    compare = service.build_compare_out(estimates)
    return compare

@router.get("/quotate", response_model=EstimateOut)
def quotate(quotation_id: int = Query(..., description="견적서 ID")):
    repo = DynamoDBRepository()
    item = repo.get_analysis_result(str(quotation_id))
    if not item:
        raise HTTPException(404, detail="quotation not found")

    # 타입 보정 후 반환
    try:
        item["quotation_id"] = int(item.get("quotation_id", quotation_id))
    except:
        item["quotation_id"] = quotation_id
    return EstimateOut.model_validate(item)

@router.get("/compare", response_model=CompareOut)
def compare(ids: List[int] = Query(...)):
    repo = DynamoDBRepository()
    id_strs = [str(i) for i in ids]
    items = repo.batch_get_analysis_results(id_strs)
    if not items:
        raise HTTPException(404, "no quotations found")

    estimates: List[EstimateOut] = []
    for it in items:
        try:
            it["quotation_id"] = int(it.get("quotation_id"))
        except:
            continue
        estimates.append(EstimateOut.model_validate(it))

    service = EstimationService(None)
    return service.build_compare_out(estimates)
