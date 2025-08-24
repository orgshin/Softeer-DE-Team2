# dynamodb_repository.py
import os
import boto3
from typing import Dict, Any
import logging

class DynamoDBRepository:
    """
    견적서 원본 및 분석 결과를 DynamoDB에 저장하는 리포지토리
    """
    def __init__(self):
        try:
            self.region = os.getenv("AWS_REGION")
            self.input_table_name = os.getenv("DYNAMODB_INPUT_TABLE")
            self.result_table_name = os.getenv("DYNAMODB_RESULT_TABLE")

            if not all([self.region, self.input_table_name, self.result_table_name]):
                raise ValueError("AWS DynamoDB 관련 환경 변수가 설정되지 않았습니다.")

            # Boto3 DynamoDB 리소스 클라이언트 생성
            self.dynamodb = boto3.resource(
                'dynamodb',
                aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
                region_name=self.region
            )
            self.input_table = self.dynamodb.Table(self.input_table_name)
            self.result_table = self.dynamodb.Table(self.result_table_name)
            logging.info("DynamoDB 리포지토리 초기화 성공")

        except Exception as e:
            logging.error(f"DynamoDB 리포지토리 초기화 실패: {e}")
            # DynamoDB를 사용할 수 없을 때 서비스가 중단되지 않도록 None으로 설정
            self.input_table = None
            self.result_table = None

    def save_user_input(self, quote_id: str, user_input: Dict[str, Any]) -> None:
        """
        사용자의 원본 견적서 입력을 DynamoDB에 저장합니다.
        """
        if not self.input_table:
            logging.warning("DynamoDB input_table이 초기화되지 않아 저장을 건너뜁니다.")
            return
            
        try:
            item = {
                'quote_id': quote_id,
                'data': user_input
            }
            self.input_table.put_item(Item=item)
            logging.info(f"사용자 입력 저장 성공 (quote_id: {quote_id})")
        except Exception as e:
            logging.error(f"DynamoDB 사용자 입력 저장 실패 (quote_id: {quote_id}): {e}")

    def save_analysis_result(self, quote_id: str, analysis_result: Dict[str, Any]) -> None:
        """
        견적서 분석 결과를 DynamoDB에 저장합니다.
        """
        if not self.result_table:
            logging.warning("DynamoDB result_table이 초기화되지 않아 저장을 건너뜁니다.")
            return

        try:
            item = {
                'quote_id': quote_id,
                **analysis_result # analysis_result 딕셔너리의 모든 키-값을 최상위 레벨에 추가
            }
            self.result_table.put_item(Item=item)
            logging.info(f"분석 결과 저장 성공 (quote_id: {quote_id})")
        except Exception as e:
            logging.error(f"DynamoDB 분석 결과 저장 실패 (quote_id: {quote_id}): {e}")