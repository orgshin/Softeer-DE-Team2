from typing import Any, Dict, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

class OfficialFeeRepository:
    def __init__(self, db: Session):
        self.db = db

    def fetch_rows(self) -> List[Dict[str, Any]]:
        q = text("SELECT task_name, price, vehicle FROM official_fee")
        rows = self.db.execute(q).mappings().all()
        return [dict(r) for r in rows]
