# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from plugins.DingdingAlert import DingdingAlert
import requests
import logging


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

ALI_SERVER_LIST = [
    ('10.52.5.218', 8083)
    ('10.52.5.219', 8083)
    ('10.52.5.219', 18083)
]

CONNECTORS_URL = 'http://%s:%s/connectors'
STATUS_URL = 'http://%s:%s/connectors/%s/status'
TASK_RESTART_URL = 'http://%s:%s/connectors/%s/tasks/%s/restart'

def connectors_status_check():
    SERVER_LIST = ALI_SERVER_LIST

    for server_ip,server_port in SERVER_LIST:
        url = CONNECTORS_URL % (server_ip, server_port)
        r = requests.get(url)
        connectors = r.json()
        for connector in connectors:
            c_url = STATUS_URL % (server_ip, server_port, connector)
            logging.info('connector url %s', c_url)
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
                    # 重启任务
                    restart_url = TASK_RESTART_URL % (server_ip, server_port, connector, task['id'])
                    requests.post(restart_url)
                    logging.info('ip %s, port %s,  connector %s, task id %d restart. url %s', server_ip, server_port, connector, task['id'], restart_url)


connectors_status = PythonOperator(
    task_id='connectors_status',
    python_callable=connectors_status_check,
    dag=dag)
