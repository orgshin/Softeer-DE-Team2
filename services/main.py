# main.py
from fastapi import FastAPI
from api import router
from database import engine
from models import Base

# 데이터베이스에 테이블 생성
# (Alembic과 같은 마이그레이션 툴을 사용하는 것이 더 권장됩니다.)
Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="견적 비교 서비스 API",
    description="입력된 자동차 수리 견적서를 분석하고 여러 견적서를 비교하여 최적의 선택을 제안합니다.",
    version="2.0.0",
)

app.include_router(router)