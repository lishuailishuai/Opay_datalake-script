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

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwd_oride_coupon_base_df_prev_day_task = UFileSensor(
    task_id='dependence_dwd_oride_coupon_base_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_coupon_base_df/country_code='nal'",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwm_oride_coupon_sum_day"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dwm_oride_coupon_sum_day_task = HiveOperator(

    task_id='dwm_oride_coupon_sum_day_task',
    hql='''
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
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag)


# 生成_SUCCESS
def check_success(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"table":"{dag_name}".format(dag_name=dag_ids),"hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds,hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)

touchz_data_success= PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_coupon_base_df_prev_day_task >> \
sleep_time >> \
dwm_oride_coupon_sum_day_task >> \
touchz_data_success
