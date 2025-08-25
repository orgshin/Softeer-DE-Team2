from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

# 사용할 Slack Connection ID를 상단에 정의합니다.
SLACK_CONN_ID = "conn_slack_webhook"


def send_slack_alert_on_failure(context):
    """
    재사용 가능한 Task 실패 알림 함수.
    모든 DAG에서 import하여 사용할 수 있습니다.
    """
    # context에서 정보 추출
    dag_id = context["dag_run"].dag_id
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url

    # 메시지 포맷팅
    message = f"""
    *🚨 Airflow Task 실패 알림 🚨*

    - *DAG*: `{dag_id}`
    - *Task*: `{task_id}`
    - *실행 시각*: `{context['ts']}`

    실패한 Task의 로그를 확인해주세요.
    *<{log_url} | 📄 로그 바로가기>*
    """

    # SlackWebhookHook을 사용하여 메시지 전송
    hook = SlackWebhookHook(slack_webhook_conn_id=SLACK_CONN_ID)
    hook.send(text=message)