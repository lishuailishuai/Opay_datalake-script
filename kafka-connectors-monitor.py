# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from plugins.DingdingAlert import DingdingAlert
import requests

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 10, 23),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}
dag = airflow.DAG(
    'kafka-connectors-monitor',
    schedule_interval="*/10 * * * *",
    default_args=args)

SERVER_LIST = [
    '10.52.48.92',
    '10.52.60.235'
]
CONNECTORS_URL = 'http://%s:8083/connectors'
STATUS_URL = 'http://%s:8083/connectors/%s/status'

def connectors_status_check():
    for server_ip in SERVER_LIST:
        url = CONNECTORS_URL % server_ip
        r = requests.get(url)
        connectors = r.json()
        for connector in connectors:
            c_url = STATUS_URL % (server_ip, connector)
            c_r = requests.get(c_url)
            content = c_r.json()
            tasks = content['tasks']
            for task in tasks:
                if task['state'] != 'RUNNING':
                    #钉钉报警
                    dingding_alet = DingdingAlert('https://oapi.dingtalk.com/robot/send?access_token=928e66bef8d88edc89fe0f0ddd52bfa4dd28bd4b1d24ab4626c804df8878bb48')
                    msg="""
                        DW kafka connectors状态异常，请检查。
                        {connector}, task_len:{task_len}, error_task_id:{task_id}, error_msg:{error_msg}
                    """.format(
                        connector=connector,
                        task_len=len(tasks),
                        task_id=task['id'],
                        error_msg=task['trace']
                    )
                    dingding_alet.send(msg)

connectors_status = PythonOperator(
    task_id='connectors_status',
    python_callable=connectors_status_check,
    dag=dag)
