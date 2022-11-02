from airflow import DAG
from airflow.operators.bash  import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator

import pandas as pd
import datetime
import pendulum
import os

FILENAME='tenantDetails_30.csv'

SLACK_CONN_ID = 'slack'
DAG_NAME = "workday-env-search-poc"
SCHEDULE_INTERVAL = '0 4 * * *'

def dag_success_callback(context):
    slack_conn_id = "slack"
    slack_webhook_token = BaseHook.get_connection(slack_conn_id).password
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :airflow-new: DAG has succeeded.
            *Task*: {context.get('task_instance').task_id}
            *Dag*: {context.get('task_instance').dag_id}
            *Execution Time*: {context.get('execution_date')}
            <{log_url}| *Log URL*>
            """
    slack_alert = SlackWebhookOperator(
        task_id="slack_test",
        http_conn_id="slack",
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username="airflow",
    )
    return slack_alert.execute(context=context)


#start
def send_slack_notification(message='', attachments=None, channel=None):
    """Send message to Slack.
    message: Text of the message to send. See below for an explanation of
    formatting. This field is usually required, unless you're providing only
    attachments instead. Provide no more than 40,000 characters or risk truncati
    attachments: [list of max 100] dict[s] slack message attachment[s]
    see https://api.slack.com/docs/message-attachments#attachment_structure
    channel:  a channel in your workspace starting with `#`
    """
    assert isinstance(message, str) and message or attachments
    if isinstance(attachments, dict):
        attachments = [attachments]

    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    print('Slack webhook failed....')
    print(slack_webhook_token)

    notification_operator = SlackWebhookOperator(
        task_id='slack_failed',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        icon_url='https://github.com/apache/airflow/raw/v1-10-stable/airflow/www/static/pin_100.png',
        message=message,
        attachments=attachments
    )
    return notification_operator.execute(attachments)

def failed_task_slack_notification(kwargs):
    """Send failed task notification with context provided by operator."""
    print(kwargs['ti'])
    dag = kwargs['ti'].dag_id
    run_id = kwargs['run_id']
    task = kwargs['ti'].task_id
    exec_date = kwargs['execution_date']
    try_number = kwargs['ti'].try_number - 1
    max_tries = kwargs['ti'].max_tries + 1
    exception = kwargs['exception']
    log_url = kwargs['ti'].log_url
    # command = kwargs['ti'].command(),

    message = (
        f'`DAG`  {dag}'
        f'\n`Run Id`  {run_id}'
        f'\n`Task`  {task} _(try {try_number} of {max_tries})_'
        f'\n`Execution`  {exec_date}'
        f'\n```{exception}```'
        # f'`Command`  {command}\n'
    )

    attachments = {
        'mrkdwn_in': ['text', 'pretext'],
        'pretext': ':red_circle: *Task Failed*',
        'title': 'airflow-web.wheely-dev.com',
        'title_link': f'https://airflow-web.wheely-dev.com',
        'text': message,
        'actions': [
            {
                'type': 'button',
                'name': 'view log',
                'text': 'View log :airflow:',
                'url': log_url,
                'style': 'primary',
            },
        ],
        'color': 'danger',  # 'good', 'warning', 'danger', or hex ('#439FE0')
        'fallback': 'details',  # Required plain-text summary of the attachment
    }

    send_slack_notification(attachments=attachments)
#end


def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('s3_conn')
    file_name = hook.download_file(key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name

def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'on_success_callback': dag_success_callback,
    'on_failure_callback': failed_task_slack_notification
}

with DAG(
    dag_id='workday_env_search_poc1',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60)
) as dag:

    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'key': 'minerva/airflow-data/tenantDetails_30.csv',
            'bucket_name': Variable.get("S3_BUCKET"),
            'local_path': '/opt/airflow/data'
        }
    )

    # Rename the file
    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': FILENAME
        }
    )

    load_wta_env_tenants_to_db = MySqlOperator(
        mysql_conn_id='mysql_conn',
        task_id='load_wta_env_tenants_to_db',
        sql='WTA_ENV_TENANTS_insert.sql',
        dag=dag
    )

    task_download_from_s3 >> task_rename_file >> load_wta_env_tenants_to_db
  
if __name__ == "__main__":
    dag.cli()
