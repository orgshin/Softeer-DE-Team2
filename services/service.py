# service.py
from __future__ import annotations
import json
import os
import re
import uuid
from typing import Any, Dict, List, Optional
from sqlalchemy.orm import Session
from fastapi import Depends, HTTPException
from repository import PartsRepository, QuoteRepository
from database import get_db
import google.generativeai as genai
import logging

class RepairService:
    def __init__(
        self,
        parts_repo: PartsRepository,
        quote_repo: QuoteRepository,
        gemini_model_name: str = "gemini-2.5-flash",
    ) -> None:
        self.parts_repo = parts_repo
        self.quote_repo = quote_repo
        # 상세 정보를 포함한 공임비 리스트를 로드합니다.
        self.fee_standard_list = self.parts_repo.get_all_fees()
        # 상세 정보 전체를 key-value로 저장합니다 (key는 label).
        self.fee_standard_dict = {item['label']: item for item in self.fee_standard_list}

        self.gemini_api_key = os.getenv("GEMINI_API_KEY")
        self.gemini_model = None
        if self.gemini_api_key:
            try:
                genai.configure(api_key=self.gemini_api_key)
                self.gemini_model = genai.GenerativeModel(gemini_model_name)
            except Exception as e:
                print(f"Gemini 모델 초기화 실패: {e}")
                self.gemini_model = None

    def _analyze_item(
        self,
        value: int,
        market_price: Optional[float],
        average_price: Optional[float],
        item_name: str,
        item_type: str
    ) -> Dict[str, Any]:
        gap_pct: Optional[float] = None
        gap_price: Optional[float] = None
        
        comparison_price = average_price
        price_type = "시장 평균가"
        
        if not comparison_price or comparison_price == 0:
            comparison_price = market_price
            price_type = "공식 가격"

        if comparison_price and comparison_price > 0:
            gap_pct = ((value - comparison_price) / comparison_price) * 100
            gap_price = value - comparison_price

        threshold = 20
        if not comparison_price or comparison_price == 0:
            tag = {"action": "proceed", "reason": "no_data"}
            review = f"{item_name}은(는) 비교 가능한 가격 데이터가 없어 시세 판단이 어렵습니다."
        elif gap_pct is not None and gap_pct > threshold:
            tag = {"action": "requote", "reason": "over_market"}
            review = f"{item_name}은(는) {price_type} 대비 {round(gap_pct, 1)}% 높으므로 재견적을 권장합니다."
        else:
            tag = {"action": "proceed", "reason": "within_market"}
            review = f"{item_name}은(는) {price_type} 대비 적정한 수준입니다."

        return {
            "name": item_name,
            "value": value,
            "market_price": market_price,
            "average": average_price,
            "gap_price": gap_price,
            "gap_pct": round(gap_pct, 1) if gap_pct is not None else None,
            "tag": tag,
            "one_line_review": review,
        }

    def _match_fees_with_gemini(self, input_fee_names: List[str]) -> Dict[str, Optional[str]]:
        if not self.gemini_model or not input_fee_names:
            return {name: None for name in input_fee_names}

        # ✨ [수정] LLM에게 '참고 정보'와 '정답 선택지'를 명확히 분리하여 전달합니다.
        # 1. LLM이 문맥을 파악하는 데 사용할 상세 정보
        standard_fees_context = self.fee_standard_list 
        # 2. LLM이 최종적으로 반환해야 할 값의 목록 (정확한 label 리스트)
        standard_fee_labels = list(self.fee_standard_dict.keys())

        prompt = f"""
# 역할
당신은 자동차 정비 용어 매칭 전문가입니다.

# 지시문
'입력 공임 리스트'의 각 항목을 '표준 공임 상세정보'를 참고하여, '정답 선택지' 리스트에서 의미적으로 가장 유사한 항목과 짝지어 주세요.

# 입력 공임 리스트:
{json.dumps(input_fee_names, ensure_ascii=False)}

# 표준 공임 상세정보 (판단 근거로만 사용):
{json.dumps(standard_fees_context, ensure_ascii=False, indent=2)}

# 정답 선택지 (반드시 이 리스트에 있는 값 중 하나를 선택해야 함):
{json.dumps(standard_fee_labels, ensure_ascii=False)}

# 출력 규칙:
1. 최종 출력은 반드시 JSON 객체 형식이어야 합니다.
2. '입력 공임 리스트'의 각 항목이 JSON의 key가 됩니다.
3. key에 해당하는 value는 반드시 **'정답 선택지' 리스트에 있는 값**이어야 합니다.
4. 만약 의미적으로 유사한 항목이 없다면, value를 null로 설정하세요.
5. 다른 설명 없이 JSON 객체만 반환해 주세요.
"""
        try:
            response = self.gemini_model.generate_content(prompt)
            text = getattr(response, "text", "") or ""
            json_match = re.search(r"```json\s*([\s\S]*?)\s*```", text)
            if json_match:
                clean_json_str = json_match.group(1)
            else:
                json_match = re.search(r"{\s*.*?\s*}", text, re.DOTALL)
                clean_json_str = json_match.group(0) if json_match else text

            matched = json.loads(clean_json_str)
            
            # 최종 결과 검증 (안전장치)
            validated_result: Dict[str, Optional[str]] = {}
            for input_name in input_fee_names:
                matched_label = matched.get(input_name)
                # LLM이 반환한 값이 '정답 선택지'에 있는지 한번 더 확인
                if isinstance(matched_label, str) and matched_label in self.fee_standard_dict:
                    validated_result[input_name] = matched_label
                else:
                    validated_result[input_name] = None
            
            print("Gemini Matching Result (Validated):", validated_result)
            return validated_result
            
        except Exception as e:
            print(f"Gemini 공임 매칭 중 오류 발생: {e}")
            return {name: None for name in input_fee_names}

    def analyze_and_save_quote(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.gemini_model:
            raise HTTPException(status_code=500, detail="Gemini API is not configured or available.")

        parts: List[Dict[str, Any]] = data.get("parts", [])
        fees: List[Dict[str, Any]] = data.get("fee", [])
        
        analyzed_parts = []
        analyzed_fees = []
        total_value = sum(p['value'] for p in parts) + sum(f['value'] for f in fees)
        total_base = 0

        for part in parts:
            code, value = str(part["code"]), int(part["value"])
            avg_price = self.parts_repo.get_part_avg_price(code)
            mobis_price = self.parts_repo.get_mobis_part_price(code)
            analyzed = self._analyze_item(value, mobis_price, avg_price, code, "part")
            analyzed_parts.append(analyzed)
            total_base += avg_price or mobis_price or value

        if fees:
            input_fee_names = [str(f["name"]) for f in fees]
            fee_mapping = self._match_fees_with_gemini(input_fee_names)
            for fee in fees:
                name, value = str(fee["name"]), int(fee["value"])
                matched_label = fee_mapping.get(name)
                standard_fee_info = self.fee_standard_dict.get(matched_label) if matched_label else None
                standard_fee_value = standard_fee_info.get('value') if standard_fee_info else None
                analyzed = self._analyze_item(value, standard_fee_value, standard_fee_value, name, "fee")
                analyzed_fees.append(analyzed)
                total_base += standard_fee_value or value

        if total_base > 0:
            total_gap_pct = ((total_value - total_base) / total_base) * 100
            if total_gap_pct > 20:
                review = f"총액이 평균가 대비 {round(total_gap_pct, 1)}% 과잉 책정됐으므로 다른 정비소 방문을 권장합니다."
            else:
                review = "총액이 평균가 대비 합리적인 수준입니다."
        else:
            review = "비교 데이터 부족으로 총평을 제공하기 어렵습니다."

        result = {
            "total": total_value,
            "one_line_review": review,
            "detail": {"parts": analyzed_parts, "fee": analyzed_fees},
        }
        
        quote_id = str(uuid.uuid4())
        self.quote_repo.save(quote_id, result)
        return {"quote_id": quote_id, "analysis": result}

    def compare_quotes(self, quote_ids: List[str]) -> Dict[str, Any]:
        raise NotImplementedError("compare_quotes is not implemented for DB repository yet.")

# --- DI 헬퍼 ---
def get_service(db: Session = Depends(get_db)) -> RepairService:
    parts_repo = PartsRepository(db)
    quote_repo = QuoteRepository(db)
    return RepairService(parts_repo=parts_repo, quote_repo=quote_repo)
