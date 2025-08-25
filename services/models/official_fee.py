from sqlalchemy import Column, Integer, String, Float, Text
from models.base import Base

class OfficialFee(Base):
    __tablename__ = "official_fee"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_name = Column(String(255), nullable=False)
    price = Column(Float, nullable=False)
    vehicle = Column(Text, nullable=True)  # JSON/문자열 모두 저장 가능
