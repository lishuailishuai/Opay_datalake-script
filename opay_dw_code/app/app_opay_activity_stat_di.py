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
    'owner': 'xiedong',
    'start_date': datetime(2019, 11, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_activity_stat_di',
                  schedule_interval="30 03 * * *",
                  default_args=args,
                  catchup=False)



##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_ods_sqoop_base_preferential_record_di_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_preferential_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_activity/preferential_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_activity_stat_di"
hdfs_path = "ufile://opay-datalake/opay/opay_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def app_opay_activity_stat_di_sql_task(ds):

    HQL='''
   
    set hive.exec.dynamic.partition.mode=nonstrict;
    set 
    INSERT overwrite TABLE {db}.{table} partition('NG', '{pt}')
    select 
        activity_id, 
        'USER' as user_type,
        'customer' as user_role,
        sum(order_amount) order_amt, 
        sum(benefit_amount) benifit_amt, 
        count(distinct order_no) order_cnt,
        count(distinct user_id) user_cnt
    from (
        select 
             activity_id, user_id, 'USER' as user_type, 'customer' as customer,
             order_no, order_amount, benefit_amount,
             row_number() over(partition by order_no order by update_time desc) rn
        from opay_dw_ods.ods_sqoop_base_preferential_record_di
        where dt = '{pt}'
    ) t1 where t1.rn = 1 
    group by activity_id

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
    _sql = app_opay_activity_stat_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opay_activity_stat_di_task = PythonOperator(
    task_id='app_opay_activity_stat_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_ods_sqoop_base_preferential_record_di_prev_day_task>>app_opay_activity_stat_di_task