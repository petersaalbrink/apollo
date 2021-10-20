from __future__ import annotations

from datetime import datetime

from airflow.models.taskinstance import Context
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from pendulum.tz import timezone


def send_message(message: str) -> None:
    return SlackWebhookOperator(
        task_id="message",
        http_conn_id="slack",
        message=message,
        channel="#airflow",
    ).execute(context=None)


def _slack_alert(context: Context, task_id: str) -> None:
    if task_id == "pass":
        first_line = ":large_green_circle: Task Passed."
    elif task_id == "fail":
        first_line = ":red_circle: Task Failed."
    else:
        raise ValueError(task_id)
    msg = f"""
        {first_line}
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {context.get('execution_date')}
        *Log Url*: <{context.get('task_instance').log_url}|log url>
    """
    return SlackWebhookOperator(
        task_id=task_id,
        http_conn_id="slack",
        message=msg,
        channel="#airflow",
    ).execute(context=context)


def task_fail_slack_alert(context: Context) -> None:
    return _slack_alert(context, "fail")


def task_pass_slack_alert(context: Context) -> None:
    return _slack_alert(context, "pass")


tz = timezone("Europe/Amsterdam")
default_args = {
    "catchup": True,
    "concurrency": 1,
    "depends_on_past": False,
    "email": [
        f"{usr}@matrixiangroup.com" for usr in ("esezgin", "psaalbrink", "tsmeitink")
    ],
    "email_on_failure": True,
    "max_active_runs": 1,
    "on_failure_callback": task_fail_slack_alert,
    "on_success_callback": task_pass_slack_alert,
    "owner": "airflow",
    "retries": 0,
    "start_date": datetime(2020, 3, 1, tzinfo=tz),
}
