from sqlalchemy import Column, Integer, String, Float
from models.base import Base

class MobisPart(Base):
    __tablename__ = "mobis_parts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(100), nullable=True, index=True)
    part_no = Column(String(100), nullable=True, index=True)  # 혹시 DB에 있는 경우
    name_raw = Column(String(255), nullable=True)
    price = Column(Float, nullable=True)
