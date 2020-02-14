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
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_shop_target_import_d',
                  schedule_interval="30 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

app_opos_shop_target_d_task = OssSensor(
    task_id='app_opos_shop_target_d_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/app_opos_shop_target_d",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 删除mysql中15天前的数据---------------------------------------##

def delete_before_day15(ds, **kargs):
    delete_sql = """
    DELETE FROM opos_dw.app_opos_shop_target_d WHERE dt<'{before_15_day}';
                """.format(
            pt=ds,
            before_1_day=airflow.macros.ds_add(ds, -1),
            before_15_day=airflow.macros.ds_add(ds, -15)
    )

    opos_mysql_cursor.execute(delete_sql)
    opos_mysql_conn.commit()

delete_before_day15_task = PythonOperator(
    task_id='delete_before_day15_task',
    python_callable=delete_before_day15,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 将最新数据插入到mysql ---------------------------------------##

app_opos_shop_target_d_task >> delete_before_day15_task








