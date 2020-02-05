# -*- coding: utf-8 -*-
import airflow
import time
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors import OssSensor
from airflow.hooks.mysql_hook import MySqlHook
import json
import logging
from airflow.models import Variable
import requests
import os

opos_mysql_hook = MySqlHook("mysql_dw")
opos_mysql_conn = opos_mysql_hook.get_conn()
opos_mysql_cursor = opos_mysql_conn.cursor()

args = {
    'owner': 'yuanfeng',
    'start_date': datetime(2019, 11, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    #'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_shop_target_week_import_d',
                  schedule_interval="30 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

app_opos_shop_target_week_w_task = OssSensor(
    task_id='app_opos_shop_target_week_w_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/app_opos_shop_target_week_w",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 判断是否是周一 ---------------------------------------##

def judge_monday(ds, **kargs):
    week = time.strptime(ds, "%Y-%m-%d")[6]
    delete_sql = ''
    # 判断是否是周一并生成对应sql
    if week == 0:
        delete_sql = """
    DELETE FROM opos_dw.app_opos_shop_target_week_w WHERE dt='{pt}';
                """.format(
            pt=ds,
            before_1_day=airflow.macros.ds_add(ds, -1)
        )

    else:
        delete_sql = """
    DELETE FROM opos_dw.app_opos_shop_target_week_w WHERE dt='{before_1_day}';
                """.format(
            pt=ds,
            before_1_day=airflow.macros.ds_add(ds, -1)
        )

    opos_mysql_cursor.execute(delete_sql)
    opos_mysql_conn.commit()

judge_monday_task = PythonOperator(
    task_id='judge_monday_task',
    python_callable=judge_monday,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 删除mysql昨日数据,当当日数据是周一时,不删除 ---------------------------------------##

app_opos_shop_target_week_w_task >> judge_monday_task









