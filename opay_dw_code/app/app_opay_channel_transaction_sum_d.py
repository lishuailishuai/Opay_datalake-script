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
    'start_date': datetime(2020, 2, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_channel_transaction_sum_d',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opay_channel_transaction_base_di_prev_day_task = OssSensor(
    task_id='dwd_opay_channel_transaction_base_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_channel_transaction_base_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
ods_sqoop_base_card_bin_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_card_bin_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_channel/card_bin",
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

table_name = "app_opay_channel_transaction_sum_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_channel_transaction_sum_d_sql_task(ds):
    HQL = '''
    
    
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table {db}.{table} partition (dt)
     
        SELECT pay_channel,
               out_channel_id,
               supply_item,
               response_code,
               transaction_status,
               nvl(b.flag,0) flag,
               user_type,
               sum(amount) AS tran_amt,
               count(1) AS tran_c,
               c.brand,
               c.bank_name,
               a.dt
        FROM
          (SELECT pay_channel,out_channel_id,supply_item,response_code,transaction_status,user_type,dt,bank_response_message,
                  amount,
                  cast(substr(AES_DECRYPT(UNHEX(bank_card_no), UNHEX('4132E08EA055A2B852DE8C214C885C2A')),1,6) as STRING) bank
           FROM opay_dw.dwd_opay_channel_transaction_base_di
           WHERE dt='{pt}'
             AND create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23') ) a
        LEFT JOIN opay_dw.dim_opay_bank_response_message_df b ON a.bank_response_message=b.bank_response_message
        LEFT JOIN (select * from opay_dw_ods.ods_sqoop_base_card_bin_df where dt='{pt}') c on a.bank=c.bin
        GROUP BY pay_channel,
                 out_channel_id,
                 supply_item,
                 response_code,
                 transaction_status,
                 nvl(b.flag,0),
                 user_type,
                 c.brand,
                 c.bank_name,
                 a.dt

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_channel_transaction_sum_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_channel_transaction_sum_d_task = PythonOperator(
    task_id='app_opay_channel_transaction_sum_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_channel_transaction_base_di_prev_day_task >> app_opay_channel_transaction_sum_d_task
ods_sqoop_base_card_bin_df_prev_day_task >> app_opay_channel_transaction_sum_d_task

