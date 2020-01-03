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
from airflow.sensors import OssSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2019, 10, 28),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_coupon_sum_day',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  catchup=False)
##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name = "dwm_oride_coupon_sum_day"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖前一天分区
    dependence_dwd_oride_coupon_base_df_prev_day_task = UFileSensor(
        task_id='dependence_dwd_oride_coupon_base_df_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_coupon_base_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    # 依赖前一天分区
    dependence_dwd_oride_coupon_base_df_prev_day_task = OssSensor(
        task_id='dependence_dwd_oride_coupon_base_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_coupon_base_df/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def dwm_oride_coupon_sum_day_sql_task(ds):
    HQL='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT coupon_type,
       amount,
       start_price,
       discount,
       city_id,
       product_id,
       CASE
           WHEN used_date=tran_date THEN '1'
           ELSE '0'
       END,
       sum(CASE
               WHEN receive_date='{pt}' THEN 1
               ELSE 0
           END),--领取量
       sum(CASE
              WHEN used_date='{pt}' THEN 1
              ELSE 0
           END),--使用量
       count(DISTINCT CASE
                    WHEN used_date='{pt}' THEN user_id
                END),--使用人数
       count(CASE
           WHEN used_date='{pt}' THEN 1
            END) ,--交易笔数
       sum(CASE
            WHEN used_date='{pt}' THEN price
           END),--应付交易金额
       sum(CASE
            WHEN used_date='{pt}' THEN amount
          END),--实付交易金额
       sum(CASE
           WHEN used_date='{pt}' THEN coupon_amount
          END), --优惠券金额
       country_code,
       dt
FROM
  (SELECT coupon_type,
          start_price,
          discount,
          city_id,
          product_id,
          price,
          amount,
          user_id,
          coupon_amount,
          country_code,
          dt,
          from_unixtime(receive_time,'yyyy-MM-dd') receive_date,
          from_unixtime(used_time,'yyyy-MM-dd') used_date,
          from_unixtime(tran_time,'yyyy-MM-dd') tran_date
   FROM oride_dw.dwd_oride_coupon_base_df
   WHERE dt='{pt}' )m
GROUP BY dt,
         coupon_type,
         amount,
         start_price,
         discount,
         country_code,
         city_id,
         product_id,
         CASE
             WHEN used_date=tran_date THEN '1'
             ELSE '0'
         END ;

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
    _sql = dwm_oride_coupon_sum_day_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_oride_coupon_sum_day_task = PythonOperator(
    task_id='dwm_oride_coupon_sum_day_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_coupon_base_df_prev_day_task>>dwm_oride_coupon_sum_day_task