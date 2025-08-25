from typing import Any, Dict, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

class RepairshopRepository:
    def __init__(self, db: Session):
        self.db = db

    def fetch_repairshop(self, name: str, address: Optional[str]) -> Optional[Dict[str, Any]]:
        if address:
            q1 = text("""
                SELECT * FROM repairshop
                WHERE shop_name = :name AND address ILIKE :addr
                ORDER BY review_count DESC
                LIMIT 1
            """)
            row = self.db.execute(q1, {"name": name, "addr": f"%{address}%"}).mappings().first()
            if row:
                return dict(row)

        q2 = text("""
            SELECT * FROM repairshop
            WHERE shop_name = :name
            ORDER BY review_count DESC
            LIMIT 1
        """)
        row2 = self.db.execute(q2, {"name": name}).mappings().first()
        return dict(row2) if row2 else None
