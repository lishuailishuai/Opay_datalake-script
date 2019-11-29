# coding: utf-8
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import requests
import bs4
from datetime import *
import logging
from plugins.comwx import ComwxApi
import json
import os
from plugins.DingdingAlert import DingdingAlert

# pushgateway外网地址
# push_gateway_address = "152.32.140.147:9091"

# pushgateway内网地址
push_gateway_address = "10.52.61.177:9091"

# pushgateway删除job restful http 接口
push_gateway_delete_restful_address = "/metrics/job"

# 格式化时间格式
time_format = "%Y-%m-%dT%H:%M:%S"

# 订订报警地址
dingding_address = "https://oapi.dingtalk.com/robot/send?access_token=3845cd5ba5cc5f9505133b5d2847d525d78026b0735e4e3c4a8707b51f718f74"

dingding_alert = DingdingAlert(dingding_address)

# ResourceManager http api接口地址
RM_HTTP_ADDRESS = "http://node5.datalake.opay.com:8088/ws/v1/cluster/apps"
RM_HTTP_PARAMS = {
    'state': 'RUNNING',
    'applicationTypes': 'Apache Flink'
}

# 任务配置情况
# 任务名称 并行度 container数量 slot数量 checkpoint statebackend路径 mainClass taskManager内存大小 , 是否从checkpoint中恢复
task_map = {
    'opay-user-order-etl': ('opay-metrics',
                            8, 4, 2,
                            's3a://opay-bi/flink/workflow/checkpoint',
                            'com.opay.bd.opay.main.OpayUserOrderMergeMain',
                            3072,
                            False
                            ),
}

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 11, 27),
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

mysql_hook = MySqlHook("mysql_dw")
mysql_conn = mysql_hook.get_conn()
mysql_cursor = mysql_conn.cursor()


# ----------------- 基于ResoureceManager的监控任务 -----------------


def insert_job_id(job_name, job_id):
    sql = """
        insert into flink_meta.flink_job_records (job_name,job_id,modify_time)
        values
        ('{job_name}','{job_id}',sysdate())

        """.format(job_name=job_name, job_id=job_id)

    mysql_cursor.execute(sql)
    mysql_conn.commit()
    results = mysql_cursor.fetchall()


def query_job_id(job_name):
    sql = """
        select 
        IFNULL(job_id,'-1')
        from 
        flink_meta.flink_job_records
        where job_name = '{job_name}'
        and modify_time = (
            select 
            max(modify_time)
            from flink_meta.flink_job_records
            where job_name = '{job_name}'
        )
    """.format(job_name=job_name)

    mysql_cursor.execute(sql)
    results = mysql_cursor.fetchall()
    if len(results) > 0:
        return str(results[0][0])
    return '-1'


def query_all_meta_job():
    sql = """
        select 
        distinct(job_name)
        from 
        flink_meta.flink_job_records
    """

    meta_jobs = set()

    mysql_cursor.execute(sql)
    results = mysql_cursor.fetchall()
    if len(results) > 0:
        for job in results:
            meta_jobs.add(str(job[0]))

    return meta_jobs


def repair_job_info(avlive_flink_job_info_map, monitor_jobs, alive_jobs):
    logging.info("========== flink application online监控任务job id 数据补齐开始 ========== ")

    meta_jobs = query_all_meta_job()
    online_jobs = monitor_jobs.intersection(alive_jobs)
    need_repair_jobs = online_jobs.difference(meta_jobs)
    for repair_job in need_repair_jobs:
        new_job_handle(avlive_flink_job_info_map, repair_job)

    logging.info("========== flink application online监控任务job id 数据补齐结束 ========== ")


def monitor_rm(ds, **kwargs):
    logging.info("========== flink application 监控任务开始 ========== ")

    # 存活任务set
    ative_flink_job = set()

    # 存活任务其他信息
    avlive_flink_job_info_map = dict()

    text = send_request(RM_HTTP_ADDRESS, RM_HTTP_PARAMS)

    obj = json.loads(text)

    if len(obj['apps']) != 0:
        apps = obj['apps']['app']
        for app in apps:
            ative_flink_job.add(app['name'])
            avlive_flink_job_info_map[app['name']] = app['trackingUrl']

    key_set = set(task_map.keys())

    repair_job_info(avlive_flink_job_info_map, key_set, ative_flink_job)

    fail_jobs = key_set.difference(ative_flink_job)
    not_monitor_jobs = ative_flink_job.difference(key_set)

    logging.info(" ative_flink_job = " + str(ative_flink_job))
    logging.info(" key_set = " + str(key_set))
    logging.info(" fail_job = " + str(fail_jobs))
    logging.info(" not_monitor_job = " + str(not_monitor_jobs))

    if len(not_monitor_jobs) > 0:
        for job in not_monitor_jobs:
            logging.info(
                "========== flink application 任务失败或此任务没有在监控列表内，请完善任务元数据信息，job = {job} ========== ".format(
                    job=job))

    if len(fail_jobs) > 0:

        logging.info("========== flink application 开始失败任务分析流程 ========== ")

        for fail_job in fail_jobs:
            job_info = task_map.get(fail_job, None)
            logging.info(
                "========== flink application 任务可能失败，job_info = {job_info} ========== ".format(job_info=job_info))
            if not job_info:
                dingding_alert.send("""Flink {job_name} : 任务失败或此任务没有在监控列表内，请完善任务元数据信息。""".format(job_name=fail_job))

                logging.info(
                    "========== flink application 任务失败或此任务没有在监控列表内，请完善任务元数据信息，job_info = {job_info} ========== ".format(
                        job_info=job_info))
                continue

            dingding_alert.send("""Flink {job_name} : 任务失败，进去启动拉起流程，正在寻找最新checkpoint。""".format(job_name=fail_job))

            # 查询失败前最新任务id
            pre_job_id = query_job_id(fail_job)

            if pre_job_id == '-1':

                dingding_alert.send("""Flink {job_name} : 任务失败，未找到前任务job id记录，从新启动新任务。""".format(job_name=fail_job))

                logging.info(
                    "========== flink application 任务失败，未找到前任务job id记录，从新启动新任务，job_name = {job_name} ========== ".format(
                        job_name=fail_job))
            else:
                dingding_alert.send(
                    """Flink {job_name} : 任务失败，进入启动拉起流程，已找到最近jobId : {pre_job_id}。""".format(job_name=fail_job,
                                                                                             pre_job_id=pre_job_id))

            # 获取job启动所需信息

            params = {
                'par_num': job_info[1],
                'container_num': job_info[2],
                'slot_num': job_info[3],
                'checkpoint_address': job_info[4],
                'main_class': job_info[5],
                'task_manager_mem_size': job_info[6],
                'is_chk': job_info[7]
            }
            logging.info(
                "========== flink application 任务失败，进入启动拉起流程 ========== ")

            new_job_id = start_flink_job(fail_job, pre_job_id, params)

            dingding_alert.send("""Flink {job_name} : 任务重新启动完成，最新jobId : {new_job_id}。""".format(job_name=fail_job,
                                                                                                 new_job_id=new_job_id))

            logging.info(
                "========== flink application 任务拉起成功========== ")

            insert_job_id(fail_job, new_job_id)

            logging.info(
                "========== flink application 任务新job id 写入成功========== ")

    logging.info("========== flink application 监控任务结束 ========== ")


def new_job_handle(avlive_flink_job_info_map, minus_job):
    logging.info("========== flink application 开始写入新任务job id ========== ")

    dingding_alert.send("""Flink {job_name} : 发现新任务没有保存job_id，正在保存。""".format(job_name=minus_job))

    respons = ''
    while respons.find('jid') < 0:
        respons = send_request("{track_url}jobs/overview".format(
            track_url=avlive_flink_job_info_map.get(minus_job)),
            None
        )
    obj = json.loads(respons)
    job_id = obj['jobs'][0]['jid']

    logging.info("minus_job = " + minus_job + "  job_id = " + job_id)

    insert_job_id(minus_job, job_id)

    dingding_alert.send("""Flink {job_name} : 已保存job_id {job_id} 。""".format(job_name=minus_job, job_id=job_id))

    logging.info("========== flink application 新任务job id 写入成功 ========== ")


def start_flink_job(job_name, pre_job_id, param):
    par_num = param['par_num']
    container_num = param['container_num']
    slot_num = param['slot_num']
    checkpoint_address = param['checkpoint_address']
    main_class = param['main_class']
    task_manager_mem_size = param['task_manager_mem_size']
    # is_chk = param['is_chk']
    checkpoint = ''
    new_job_id = ''
    lines = None

    # 获取checkpoint最新地址

    if  pre_job_id == '-1':
        command = """
                 ssh node5.datalake.opay.com " source /etc/profile ; flink run  -p {par_num} -m yarn-cluster -yn {container_num} -ytm {task_manager_mem_size} -yqu root.users.airflow -ynm {job_name} -ys {slot_num}  -d -c {main_class} bd-flink-project-1.0.jar"
                """.format(
            par_num=par_num,
            container_num=container_num,
            job_name=job_name,
            slot_num=slot_num,
            main_class=main_class,
            task_manager_mem_size=task_manager_mem_size
        )

        logging.info(command)

        lines = os.popen(command).readlines()

    else:

        command = """
            hdfs dfs -ls {checkpoint_address}/{pre_job_id}
        """.format(checkpoint_address=checkpoint_address, pre_job_id=pre_job_id)

        logging.info(command)

        lines = os.popen(command).readlines()

        logging.info(str(lines))

        for line in lines:
            if str(line).find('chk') > -1:
                line = str(line)
                checkpoint = line[line.index('chk'):].strip('\n')
                break

        dingding_alert.send(
            """Flink {job_name} : 任务失败，进去启动拉起流程，已找到最近jobId : {pre_job_id}，保存checkpoint 标识位置：{checkpoint}。""".format(
                job_name=job_name, pre_job_id=pre_job_id, checkpoint=checkpoint))

        command = """
                ssh node5.datalake.opay.com " source /etc/profile ; flink run -s {checkpoint_address}/{pre_job_id}/{checkpoint} -p {par_num}  -m yarn-cluster -yn {container_num} -ytm {task_manager_mem_size} -yqu root.users.airflow -ynm {job_name} -ys {slot_num}  -d -c {main_class} bd-flink-project-1.0.jar"
            """.format(
            checkpoint_address=checkpoint_address,
            pre_job_id=pre_job_id,
            checkpoint=checkpoint,
            par_num=par_num,
            container_num=container_num,
            job_name=job_name,
            slot_num=slot_num,
            main_class=main_class,
            task_manager_mem_size=task_manager_mem_size
        )

        logging.info(command)
        lines = os.popen(command).readlines()

    for line in lines:
        if str(line).find('Job has been submitted with JobID') > -1:
            line = str(line)
            new_job_id = line[line.index('JobID') + 5:].strip()
            break

    logging.info(" new_job_id = " + new_job_id)

    return new_job_id


def find_last_checkpoint(task_checkpoint_dir):
    hdfs_command = """
        hdfs dfs -ls {task_checkpoint_dir}
    """.format(task_checkpoint_dir=task_checkpoint_dir)

    checkpiints = os.popen(hdfs_command)
    checkpiints.read()


def send_request(address, params):
    res = requests.get(address, params)
    return res.text


def send_pushgateway_request():
    address = "http://{address}/#".format(address=push_gateway_address)
    res = requests.get(address)
    return res.text


# 解析html页面，获取心跳信息
def parseHTML():
    document = send_pushgateway_request()
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
    logging.info("========监控任务开始执行=======")

    item_list = parseHTML()
    logging.info("任务列表：" + str(item_list))

    alter_job = list()
    kill_jobs = list()

    now_time = datetime.now()

    for job in item_list:
        job_name = job[0]
        time_str = str(job[1].pop())
        time_str = time_str[:time_str.index('+')]
        job_last_avtive_time = datetime.strptime(time_str, time_format)
        delay = now_time - job_last_avtive_time
        if delay > timedelta(seconds=60 * 10):
            kill_jobs.append(job_name)
        if delay > timedelta(seconds=60 * 5):
            alter_job.append(job_name)

    alter_message = "注意："

    if len(alter_job) > 0:
        for job in alter_job:
            alter_message += """
            Flink job进程 ：{job} 最近5分钟无上报心跳，请查看任务。
            """.format(job=job)

            dingding_alert.send(alter_message)

    if len(kill_jobs) > 0:
        for job in kill_jobs:
            kill_job(job)

    logging.info("========监控任务执行完毕=======")


# 杀死超时job
def kill_job(job_name):
    request_address = """
        http://{push_gateway_address}{push_gateway_delete_restful_address}/{job_name}
    """.format(
        push_gateway_address=push_gateway_address,
        push_gateway_delete_restful_address=push_gateway_delete_restful_address,
        job_name=job_name
    )
    requests.delete(request_address)


flink_monitor_rm_task = PythonOperator(
    task_id='flink_monitor_rm_task',
    python_callable=monitor_rm,
    provide_context=True,
    dag=dag
)
#
# flink_monitor_task = PythonOperator(
#     task_id='flink_monitor_task',
#     python_callable=monitor,
#     provide_context=True,
#     dag=dag
# )
