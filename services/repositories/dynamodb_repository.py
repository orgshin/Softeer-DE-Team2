# repositories/dynamodb_repository.py
import os
import json
import boto3
import logging
from typing import Dict, Any, List, Optional
from decimal import Decimal, InvalidOperation
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger(__name__)

def _float_to_decimal(obj: Any) -> Any:
    """
    재귀적으로 dict/list 내부의 float을 Decimal로 변환한다.
    - NaN/Inf는 DynamoDB에서 허용되지 않으므로 예외 처리
    - 숫자 문자열은 그대로 두고, 실제 float 타입만 변환
    """
    from math import isfinite

    if isinstance(obj, float):
        if not isfinite(obj):
            raise ValueError("NaN/Infinity는 DynamoDB에 저장할 수 없습니다.")
        # 문자열 경유로 정밀도 보존
        return Decimal(str(obj))
    if isinstance(obj, list):
        return [_float_to_decimal(v) for v in obj]
    if isinstance(obj, dict):
        return {k: _float_to_decimal(v) for k, v in obj.items()}
    return obj

class DynamoDBRepository:
    """
    견적서 원본 및 분석 결과를 DynamoDB에 저장/조회하는 리포지토리
    - 입력 테이블 (UserInputQuotes):  quote_id(PK), data(원본 입력 전체)
    - 결과 테이블 (AnalysisResults):  quote_id(PK), [응답 스키마 필드들...]
    """
    def __init__(self):
        try:
            self.region = os.getenv("AWS_REGION")
            self.input_table_name = os.getenv("DYNAMODB_INPUT_TABLE")
            self.result_table_name = os.getenv("DYNAMODB_RESULT_TABLE")

            if not all([self.region, self.input_table_name, self.result_table_name]):
                raise ValueError("AWS DynamoDB 관련 환경 변수가 설정되지 않았습니다.")

            self.dynamodb = boto3.resource(
                "dynamodb",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=self.region,
            )
            self.client = boto3.client(
                "dynamodb",
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=self.region,
            )
            self.input_table = self.dynamodb.Table(self.input_table_name)
            self.result_table = self.dynamodb.Table(self.result_table_name)
            self._deserializer = TypeDeserializer()

            logging.info("DynamoDB 리포지토리 초기화 성공")
        except Exception as e:
            logging.error(f"DynamoDB 리포지토리 초기화 실패: {e}")
            self.input_table = None
            self.result_table = None
            self.client = None
            self._deserializer = None

    # ---------------- 저장 ----------------

    def save_user_input(self, quote_id: str, user_input: Dict[str, Any]) -> None:
        """
        요청 바디 전체를 그대로 저장 (float → Decimal 변환)
        """
        if not self.input_table:
            logging.warning("DynamoDB input_table 미초기화 → 저장 스킵")
            return
        try:
            item = {
                "quote_id": quote_id,
                "data": _float_to_decimal(user_input),
            }
            self.input_table.put_item(Item=item)
            logging.info(f"사용자 입력 저장 성공 (quote_id={quote_id})")
        except Exception as e:
            logging.error(f"DynamoDB 사용자 입력 저장 실패 (quote_id={quote_id}): {e}")

    def save_analysis_result(self, quote_id: str, analysis_result: Dict[str, Any]) -> None:
        """
        응답 스키마를 최상위로 그대로 저장 (float → Decimal 변환)
        """
        if not self.result_table:
            logging.warning("DynamoDB result_table 미초기화 → 저장 스킵")
            return
        try:
            item = {"quote_id": quote_id, **_float_to_decimal(analysis_result)}
            self.result_table.put_item(Item=item)
            logging.info(f"분석 결과 저장 성공 (quote_id={quote_id})")
        except Exception as e:
            logging.error(f"DynamoDB 분석 결과 저장 실패 (quote_id={quote_id}): {e}")

    # ---------------- 조회 ----------------

    def get_analysis_result(self, quote_id: str) -> Optional[Dict[str, Any]]:
        """단건 조회 (GET /quotate 용) — resource.Table.get_item은 자동 역직렬化(Decimal 유지)"""
        if not self.result_table:
            logging.warning("DynamoDB result_table 미초기화 → 조회 스킵")
            return None
        try:
            resp = self.result_table.get_item(Key={"quote_id": quote_id})
            return resp.get("Item")
        except ClientError as e:
            logging.error(f"DynamoDB get_item 실패 (quote_id={quote_id}): {e}")
            return None

    def batch_get_analysis_results(self, quote_ids: List[str]) -> List[Dict[str, Any]]:
        """
        다건 조회 (GET /compare 용). 최대 100개씩 batch.
        boto3 client.batch_get_item은 AttributeValue 형태로 반환하므로 TypeDeserializer로 복원.
        """
        if not self.client or not self.result_table_name:
            logging.warning("DynamoDB client/result_table 미초기화 → 조회 스킵")
            return []

        out: List[Dict[str, Any]] = []
        for i in range(0, len(quote_ids), 100):
            chunk = quote_ids[i : i + 100]
            keys = [{"quote_id": {"S": qid}} for qid in chunk]
            try:
                resp = self.client.batch_get_item(
                    RequestItems={self.result_table_name: {"Keys": keys}}
                )
                items = resp.get("Responses", {}).get(self.result_table_name, [])
                # AttributeValue(dict) → Python(dict with Decimal)
                for av in items:
                    py_item = {k: self._deserializer.deserialize(v) for k, v in av.items()}
                    out.append(py_item)
            except ClientError as e:
                logging.error(f"DynamoDB batch_get_item 실패: {e}")
        return out
