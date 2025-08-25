import json
from typing import List, Dict, Any, Optional
from rapidfuzz import fuzz, process

def parse_vehicle_tokens(raw) -> List[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if x is not None]
    s = str(raw).strip()

    if s.startswith('[') and s.endswith(']') and '"' in s:
        try:
            arr = json.loads(s)
            if isinstance(arr, list):
                return [str(x).strip() for x in arr if x is not None]
        except Exception:
            pass

    if s.startswith('[') and s.endswith(']'):
        inner = s[1:-1].replace("'", "")
        return [t.strip() for t in inner.split(',') if t.strip()]

    if ',' in s:
        return [t.strip() for t in s.split(',') if t.strip()]

    return [s] if s else []

def best_fee_match(name: str, refs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    choices = [r["task_name"] for r in refs]
    if not choices:
        return None
    best = process.extractOne(name, choices, scorer=fuzz.token_set_ratio)
    if not best or best[1] < 70:
        return None
    for r in refs:
        if r["task_name"] == best[0]:
            return r
    return None
