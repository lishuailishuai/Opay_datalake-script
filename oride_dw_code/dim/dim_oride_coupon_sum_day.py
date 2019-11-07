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

dag = airflow.DAG('dim_oride_coupon_sum_day',
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
        hdfs_path_str="oride/oride_dw/dwd_oride_coupon_base_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dim_oride_coupon_sum_day"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dim_oride_coupon_sum_day_task = HiveOperator(

    task_id='dim_oride_coupon_sum_day_task',
    hql='''
    INSERT overwrite TABLE oride_dw.{table} partition(dt='{pt}')
    select 
       coupon_type,amount,start_price,discount,country_code,city_id,product_id,case when used_date=tran_date then '1' else '0' end,
       sum(case when receive_date='{pt}' then 1 else 0 end),--领取量
       sum(case when used_date='{pt}' then 1 else 0 end),--使用量
       count(distinct case when used_date='{pt}' then user_id end),--使用人数
       count(case when used_date='{pt}' then 1 end) ,--交易笔数
       sum(case when used_date='{pt}' then amt end),--交易金额
       sum(case when used_date='{pt}' then discount_amt end) --交易折扣金额
    from 
       (select coupon_type,amount,start_price,discount,country_code,city_id,product_id,amt,discount_amt,user_id,
             from_unixtime(receive_time,'yyyy-MM-dd') receive_date,
             from_unixtime(used_time,'yyyy-MM-dd') used_date,
             from_unixtime(tran_time,'yyyy-MM-dd') tran_date
       from oride_dw.dwd_oride_coupon_base_df where dt='{pt}'
      )m
  group by coupon_type,amount,start_price,discount,country_code,city_id,product_id,case when used_date=tran_date then '1' else '0' end 
;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag)


# 生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path + '/dt={{ds}}'
    ),
    dag=dag)

dependence_dwd_oride_coupon_base_df_prev_day_task >> \
sleep_time >> \
dim_oride_coupon_sum_day_task >> \
touchz_data_success
