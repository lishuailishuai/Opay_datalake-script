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
    'start_date': datetime(2019, 11, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_coupon_base_df',
                  schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

#sleep_time = BashOperator(
#   task_id='sleep_id',
#   depends_on_past=False,
#    bash_command='sleep 30',
#    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_ods_sqoop_base_data_coupon_df_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_data_coupon_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_coupon",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_ods_sqoop_base_data_order_payment_df_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_data_order_payment_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_order_payment",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_ods_sqoop_base_data_user_extend_df_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_data_user_extend_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_user_extend",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dependence_ods_sqoop_base_data_order_df_prev_day_task = UFileSensor(
    task_id='dependence_ods_sqoop_base_data_order_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_order",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##
db_name="oride_dw"
table_name = "dwd_oride_coupon_base_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_coupon_base_df_sql_task(ds):
    HQL='''
    INSERT overwrite TABLE {db}.{table} partition (country_code='nal',dt='{pt}')
    SELECT a.id,
           a.template_id,
           a.name,
           a.type,
           a.amount,
           a.max_amount,
           a.start_price,
           a.discount,
           a.city_id,
           a.serv_type,
           a.start_time,
           a.expire_time,
           a.receive_time,
           a.used_time,
           a.user_id,
           b.register_time,
           finish_d,
           a.order_id,
           c.price,
           c.amount,
           c.coupon_amount,
           c.capped_id,
           c.capped_type,
           c.capped_mode,
           a.status
    FROM
      (SELECT *
       FROM oride_dw_ods.ods_sqoop_base_data_coupon_df
       WHERE dt='{pt}') a
    LEFT JOIN
      (SELECT id,
              register_time
       FROM oride_dw_ods.ods_sqoop_base_data_user_extend_df
       WHERE dt='{pt}') b ON a.user_id=b.id
    LEFT JOIN
      (SELECT *
       FROM oride_dw_ods.ods_sqoop_base_data_order_payment_df
       WHERE dt='{pt}'
         AND status='1') c ON a.order_id=c.id
    LEFT JOIN
      (SELECT user_id,
              min(create_time) finish_d
       FROM oride_dw_ods.ods_sqoop_base_data_order_df
       WHERE dt='{pt}'
         AND status='5'
       GROUP BY user_id) d ON a.user_id=d.user_id;

'''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )
    return  HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_oride_coupon_base_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_coupon_base_df_task = PythonOperator(
    task_id='dwd_oride_coupon_base_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_ods_sqoop_base_data_coupon_df_prev_day_task>>dwd_oride_coupon_base_df_task
dependence_ods_sqoop_base_data_order_payment_df_prev_day_task>>dwd_oride_coupon_base_df_task
dependence_ods_sqoop_base_data_user_extend_df_prev_day_task>>dwd_oride_coupon_base_df_task
dependence_ods_sqoop_base_data_order_df_prev_day_task>>dwd_oride_coupon_base_df_task

