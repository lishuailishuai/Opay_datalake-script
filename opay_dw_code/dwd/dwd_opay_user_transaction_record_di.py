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
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

##
# 央行月报汇报指标
#
args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = airflow.DAG('dwd_opay_user_transaction_record_di',
                  schedule_interval="30 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opay_transfer_of_account_record_di_task = OssSensor(
    task_id='dwd_opay_transfer_of_account_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transfer_of_account_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_cico_record_di_task = OssSensor(
    task_id='dwd_opay_cico_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cico_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_transaction_record_di_prev_day_task = OssSensor(
    task_id='dwd_opay_transaction_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transaction_record_di/country_code=NG",
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
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
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
table_name = "dwd_opay_user_transaction_record_di"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name


def dwd_opay_user_transaction_record_di_sql_task(ds):
    HQL='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    set mapred.max.split.size=1000000;
    
    insert overwrite table {db}.{table} partition(country_code, dt)
    select 
        order_no, amount, currency, 
        originator_id as user_id, originator_role as user_role, 'originator' as originator_or_affiliate, 
        create_time, update_time, top_service_type, sub_service_type, order_status,
        top_consume_scenario, sub_consume_scenario,
        country_code, dt
    from opay_dw.dwd_opay_transaction_record_di 
    where dt = '{pt}' and originator_type = 'USER' 
    union all
    select 
        order_no, amount, currency, 
        affiliate_id as user_id, affiliate_role as user_role, 'affiliate' as originator_or_affiliate, 
        create_time, update_time, top_service_type, sub_service_type, order_status,
        top_consume_scenario, sub_consume_scenario,
        country_code, dt
    from opay_dw.dwd_opay_transfer_of_account_record_di
    where dt = '{pt}' and sub_service_type = 'AATransfer' and affiliate_type = 'USER'
    union all
    select 
        order_no, amount, currency, 
        affiliate_id as user_id, affiliate_role as user_role, 'affiliate' as originator_or_affiliate, 
        create_time, update_time, top_service_type, sub_service_type, order_status,
        top_consume_scenario, sub_consume_scenario,
        country_code, dt
    from opay_dw.dwd_opay_cico_record_di
    where dt = '{pt}' and affiliate_type = 'USER'
    
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL



# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opay_user_transaction_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_opay_user_transaction_record_di_task = PythonOperator(
    task_id='dwd_opay_user_transaction_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_transaction_record_di_prev_day_task >> dwd_opay_user_transaction_record_di_task
dwd_opay_transfer_of_account_record_di_task >> dwd_opay_user_transaction_record_di_task
dwd_opay_cico_record_di_task >> dwd_opay_user_transaction_record_di_task