# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors import OssSensor

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2019, 12, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opay_channel_transaction_base_di',
                  schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

ods_sqoop_base_channel_transaction_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_channel_transaction_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_channel/channel_transaction",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"

table_name = "dwd_opay_channel_transaction_base_di"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def dwd_opay_channel_transaction_base_di_sql_task(ds):
    HQL = '''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table {db}.{table} partition (dt)
    SELECT id,
       request_id,
       order_no,
       business_no,
       out_order_no,
       pay_channel,
       out_channel_id,
       user_type,
       user_id,
       transaction_status,
       step,
       step_status,
       notice_url,
       amount,
       currency,
       supply_item,
       memo,
       bank_card_no,
       card_expiry,
       security_code,
       bank_code,
       bank_response_code,
       bank_response_message,
       response_code,
       response_message,
       retry_flag,
       create_time,
       update_time,
       dt
    FROM opay_dw_ods.ods_sqoop_base_channel_transaction_di
    where dt='{pt}'

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opay_channel_transaction_base_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


dwd_opay_channel_transaction_base_di_task = PythonOperator(
    task_id='dwd_opay_channel_transaction_base_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_channel_transaction_di_prev_day_task >> dwd_opay_channel_transaction_base_di_task

