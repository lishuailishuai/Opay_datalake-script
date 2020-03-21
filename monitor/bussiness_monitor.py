# coding: utf-8
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import Variable
import logging
import os
from plugins.DingdingAlert import DingdingAlert
import paramiko
from scp import SCPClient
import time
from datetime import datetime
from datetime import *
# 格式化时间格式
time_format = "%Y-%m-%dT%H:%M:%S"

# 订订报警地址
dingding_address = "https://oapi.dingtalk.com/robot/send?access_token=3845cd5ba5cc5f9505133b5d2847d525d78026b0735e4e3c4a8707b51f718f74"

dingding_alert = DingdingAlert(dingding_address)

args = {
    'owner': 'linan',
    'start_date': datetime(2020, 3, 19),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'email': ['bigdata_dw@opay-inc.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
}

dag = airflow.DAG(
    'bussiness_monitor',
    schedule_interval="*/5 * * * *",
    default_args=args)

mysql_hook = MySqlHook("bussiness_mysql")
mysql_conn = mysql_hook.get_conn()
mysql_cursor = mysql_conn.cursor()

exec_command = """
    influx -database 'serverDB' -execute '{sql}'  -format='csv' > {metrics_name}.txt && echo 1 || echo 0
"""

ssh = paramiko.SSHClient()
key = paramiko.AutoAddPolicy()
ssh.set_missing_host_key_policy(key)
ssh.connect('10.52.5.233', 22, 'airflow', '', timeout=5)

scp = SCPClient(ssh.get_transport())

cat_command = """
    cat /home/airflow/{metrics_name}.txt && echo 1 || echo 0
"""

date_format = '%Y-%m-%d %H:%M:%S'

metrcis_list = [
    ('active_user'
     , """
    insert into
    opay_active_user_event_1(time,avtive_user_cnt)
    values('{time}',{avtive_user_cnt})
    ON DUPLICATE KEY
    UPDATE
    time=VALUES(time), 
    avtive_user_cnt=VALUES(avtive_user_cnt) 
    ;
    """
     ,
     '''SELECT count(distinct("uid")) as "active_user_cnt"  FROM "OPAY_ACTIVE_USER_EVENT"  GROUP BY time(5m)''',
     ['time', 'avtive_user_cnt']),

]


def monitor_task(ds, metrics_name, mysql_insert_sql, influx_db_query_sql, **kwargs):
    logging.info("begin ======  ")

    logging.info(exec_command.format(
        sql=influx_db_query_sql,
        metrics_name=metrics_name
    ))

    stdin, stdout, stderr = ssh.exec_command(exec_command.format(
        sql=influx_db_query_sql,
        metrics_name=metrics_name
    ))

    status = int(str(stdout.readlines()[0]).strip('\n'))

    logging.info("status = " + str(status))

    if status != 1:
        raise Exception('执行系统命令失败, command=%s, status=%s' % (exec_command.format(
            sql=influx_db_query_sql,
            metrcis_name=metrics_name
        ), status))

    logging.info("influx 执行完毕 ======  ")

    stdin, stdout, stderr = ssh.exec_command(cat_command.format(metrics_name=metrics_name))


    for line in stdout.readlines():
        logging.info("line = " + line)
        fileds = line.split(',')
        if len(fileds) >= 2:
            time = str(fileds[1])
            metrics_value = fileds[2]

            if str(time).isnumeric():
                mysql_cursor.execute(mysql_insert_sql.format(
                    time=datetime.fromtimestamp(int(time[0:10])).strftime('%Y-%m-%d %H:%M:%S'),
                    avtive_user_cnt=(metrics_value)
                ))

    logging.info("数据写入完毕 ======  ")
    mysql_conn.commit()


for metrics_name, mysql_insert_sql, influx_db_query_sql, columns in metrcis_list:
    monitor = PythonOperator(
        task_id='monitor_task_{}'.format(metrics_name),
        python_callable=monitor_task,
        provide_context=True,
        op_kwargs={
            'metrics_name': metrics_name,
            'mysql_insert_sql': mysql_insert_sql,
            'influx_db_query_sql': influx_db_query_sql
        },
        dag=dag
    )
