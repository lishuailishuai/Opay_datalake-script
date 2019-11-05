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

dag = airflow.DAG('dwd_oride_coupon_base_df',
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

table_name = "dwd_oride_coupon_base_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_coupon_base_df_task = HiveOperator(

    task_id='dwd_oride_coupon_base_df_task',
    hql='''
    INSERT overwrite TABLE oride_dw.{table} partition(dt='{pt}')
    select a.id,a.template_id,a.name,a.type,a.amount,a.max_amount,a.start_price,a.discount,a.city_id,a.serv_type,
           a.start_time,
           a.expire_time,
           a.receive_time,
           a.used_time,
           a.user_id,
           b.register_time,
           finish_d,a.order_id,c.price,c.amount,status,'NG'
    from 
        (select * from oride_dw_ods.ods_sqoop_base_data_coupon_df where dt='{pt}') a
    left join 
        (select id,register_time from oride_dw_ods.ods_sqoop_base_data_user_extend_df where dt='{pt}') b 
    on a.user_id=b.id
    left join 
        (select id,price,amount
           from oride_dw_ods.ods_sqoop_base_data_order_payment_df where dt='{pt}' and status='1') c
    on a.order_id=c.id 
    left join 
        (select user_id,min(create_time) finish_d from oride_dw_ods.ods_sqoop_base_data_order_df 
           where dt='{pt}' and status='5'  group by user_id) d
    on a.user_id=d.user_id;

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

dependence_ods_sqoop_base_data_coupon_df_prev_day_task >> \
dependence_ods_sqoop_base_data_order_payment_df_prev_day_task >> \
dependence_ods_sqoop_base_data_user_extend_df_prev_day_task >> \
dependence_ods_sqoop_base_data_order_df_prev_day_task >> \
sleep_time >> \
dwd_oride_coupon_base_df_task >> \
touchz_data_success
