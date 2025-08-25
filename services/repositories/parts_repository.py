from typing import Dict, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

class PartsRepository:
    def __init__(self, db: Session):
        self.db = db

    def get_code_column(self) -> Optional[str]:
        q = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name='mobis_parts'
        """)
        cols = {r["column_name"] for r in self.db.execute(q).mappings().all()}
        for cand in ("code", "part_no"):
            if cand in cols:
                return cand
        return None

    def fetch_refs_by_code(self, codes: List[str]) -> Dict[str, float]:
        if not codes:
            return {}
        code_col = self.get_code_column()
        if code_col is None:
            return {}
        q = text(f"""
            SELECT {code_col} AS code, AVG(price)::float AS avg_price
            FROM mobis_parts
            WHERE {code_col} = ANY(:codes) AND price IS NOT NULL
            GROUP BY {code_col}
        """)
        rows = self.db.execute(q, {"codes": codes}).mappings().all()
        return {str(r["code"]): float(r["avg_price"]) for r in rows if r["avg_price"] is not None}
