# -*- coding: utf-8 -*-
import airflow
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
import json
import logging
from airflow.models import Variable
import requests
import os

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

dag = airflow.DAG('test_mysql',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  catchup=False)

print '{{ds}}'
print '{{ds}}'[-2]

if '{{ds}}'[-2] == '01':
    delete_sql = """
            DELETE FROM opos_dw.app_test WHERE dt='{ds}';
            --DELETE FROM opos_dw.app_test WHERE dt='{before_1_day}';
        """.format(
        ds='{{ds}}',
        before_1_day='{{ macros.ds_add(ds, -1) }}'
    ),
else:
    delete_sql = """
            DELETE FROM opos_dw.app_test WHERE dt='{ds}';
            DELETE FROM opos_dw.app_test WHERE dt='{before_1_day}';
        """.format(
        ds='{{ds}}',
        before_1_day='{{ macros.ds_add(ds, -1) }}'
    )
print delete_sql

drop_mysql_yesterday_data = MySqlOperator(
    task_id='drop_mysql_yesterday_data',
    sql=delete_sql,
    mysql_conn_id='mysql_dw',
    dag=dag)


insert_mysql_today_data = HiveToMySqlTransfer(
    task_id='insert_mysql_today_data',
    sql="""
select
'1' as id
,'yuanfeng' as name
,'{ds}' as dt
from
opos_dw_ods.ods_sqoop_base_bd_city_df
where
dt='{ds}'

    """.format(
        ds='{{ds}}',
        before_1_day ='{{ macros.ds_add(ds, -1) }}'
    ),
    mysql_conn_id='mysql_dw',
    mysql_table='app_test',
    dag=dag)


drop_mysql_yesterday_data >> insert_mysql_today_data







