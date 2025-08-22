# models.py
from sqlalchemy import Column, Integer, String, Float, Text
from sqlalchemy.dialects.postgresql import JSONB
from database import Base

class Part(Base):
    """
    'parts_avg_price' 테이블 모델 (부품 시장 평균가)
    """
    __tablename__ = "parts_avg_price"
    
    id = Column(Integer, primary_key=True, index=True)
    category_raw = Column(String)
    name_raw = Column(String)
    vendor_code = Column(String, primary_key=True, index=True)
    avg_price = Column(Float)

# ✨ [신규 추가] 'mobis_parts' 테이블 모델 (부품 공식 가격)
class MobisPart(Base):
    """
    'mobis_parts' 테이블 모델 (현대모비스 공식 부품 가격)
    """
    __tablename__ = "mobis_parts"

    id = Column(Integer, primary_key=True, index=True)
    제조사 = Column(String)
    차량구분 = Column(String)
    모델 = Column(String)
    검색키워드 = Column(String)
    부품번호 = Column(String, index=True) # vendor_code와 매칭될 컬럼
    한글부품명 = Column(String)
    영문부품명 = Column(String)
    가격 = Column(Integer)

class OfficialFee(Base):
    """
    'official_fee' 테이블 모델 (표준 공임비)
    """
    __tablename__ = "official_fee"
    
    id = Column(Integer, primary_key=True, index=True)
    category_name = Column(String)
    label = Column(String, index=True)
    section_title = Column(String)
    fee_amount = Column(Integer)
    time_min = Column(Integer)
    time_max = Column(Integer)
    work_type = Column(String)

class Quote(Base):
    """
    견적 분석 결과를 저장할 'quotes' 테이블 모델
    """
    __tablename__ = "quotes"

    id = Column(String, primary_key=True, index=True)
    analysis_result = Column(JSONB)
