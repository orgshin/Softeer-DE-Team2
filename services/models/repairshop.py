from sqlalchemy import Column, Integer, String, Float
from models.base import Base

class Repairshop(Base):
    __tablename__ = "repairshop"

    id = Column(Integer, primary_key=True, autoincrement=True)
    shop_name = Column(String(255), nullable=False)
    address = Column(String(255), nullable=True)
    grade = Column(Integer, nullable=True)             # 1 ~ 3
    review_count = Column(Integer, default=0)
    positive_ratio = Column(Float, default=0.0)        # 0 ~ 1
