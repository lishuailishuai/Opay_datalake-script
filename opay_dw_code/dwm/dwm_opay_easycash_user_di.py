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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2019, 11, 7),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_easycash_user_di',
                  schedule_interval="30 03 * * *",
                  default_args=args,
                  catchup=False)



##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwd_opay_user_easycash_record_di_prev_day_task = UFileSensor(
    task_id='dependence_dwd_opay_user_easycash_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_user_easycash_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "dwm_opay_easycash_user_di"
hdfs_path = "ufile://opay-datalake/opay/opay_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def dwm_opay_easycash_user_di_sql_task(ds):

    HQL='''
   
     set hive.exec.dynamic.partition.mode=nonstrict;
   
     
    INSERT overwrite TABLE {db}.{table} partition(country_code,dt)
    SELECT user_id,
           'agent' as user_role,
           'easycash' as service_type,
            order_status,
            sum(amount) s_amount,
            count(1) c,
            country_code,
            dt
    FROM opay_dw.dwd_opay_user_easycash_record_di
    WHERE dt='{pt}' and create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')
    GROUP BY user_id,
             order_status,
             country_code,
             dt;

'''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )
    return HQL

# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_opay_easycash_user_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_opay_easycash_user_di_task = PythonOperator(
    task_id='dwm_opay_easycash_user_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)
dependence_dwd_opay_user_easycash_record_di_prev_day_task>>dwm_opay_easycash_user_di_task