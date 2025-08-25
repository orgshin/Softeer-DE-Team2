from fastapi import FastAPI
from apis.estimate import router as estimate_router

app = FastAPI(title="Estimate API v3 (SQLAlchemy, Modular)")

# 라우터 등록
app.include_router(estimate_router, prefix="")
