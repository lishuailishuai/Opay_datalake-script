# coding: utf-8
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import requests
import bs4
from datetime import *
import logging
from plugins.comwx import ComwxApi

# pushgateway外网地址
push_gateway_address = "152.32.140.147"

# pushgateway内网地址
push_gateway_address = "152.32.140.147"

# 格式化时间格式
time_format = "%Y-%m-%dT%H:%M:%S"

comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 11, 1),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'flink_monitor',
    schedule_interval="*/5 * * * *",
    default_args=args)


def send_request():
    address = "http://{address}:9091/#".format(address=push_gateway_address)
    res = requests.get(address)
    return res.text


# 解析html页面，获取心跳信息
def parseHTML():
    document = send_request()
    item_list = list()

    entity = bs4.BeautifulSoup(document, 'html5lib')
    items = entity.select("button[class='btn btn-secondary collapsed']")
    for item in items:
        content = str(item.get_text())
        if content.find('job=') > -1:
            job_name = content.replace('job=', '').replace('"', '').strip()

            item_list.append((job_name, set()))

        elif content.find('last pushed') > -1:
            content = content[content.index('last pushed:'):]
            last_active_time = content.replace('last pushed:', '').strip()
            time_set = item_list[len(item_list) - 1][1]
            time_set.add(last_active_time)

        else:
            continue

    return item_list


# 监控心跳函数
def monitor(ds, **kwargs):
    item_list = parseHTML()
    logging.info(item_list)

    alter_job = list()
    kill_job = list()

    now_time = datetime.datetime.now()

    for job in item_list:
        job_name = job[0]
        time_str = str(job[1].pop())
        time_str = time_str[:time_str.index('+')]
        job_last_avtive_time = datetime.datetime.strptime(time_str, time_format)
        delay = now_time - job_last_avtive_time
        if delay > datetime.timedelta(days=1):
            kill_job.append(job_name)
        if delay > datetime.timedelta(seconds=60 * 5):
            alter_job.append(job_name)

    alter_message = "注意："

    if len(alter_job) > 0:
        for job in alter_job:
            alter_message += """
                flink job进程 ：{job} 最近5分钟无上报心跳，请查看任务。
            """.format(job=job)
        comwx.postAppMessage(alter_message, '271')


# 杀死超时job，目前不实现
def kill_job(job_name):
    pass


flink_monitor_task = PythonOperator(
    task_id='flink_monitor_task',
    python_callable=monitor,
    provide_context=True,
    dag=dag
)
