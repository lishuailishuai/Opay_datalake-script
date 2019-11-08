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

dag = airflow.DAG('dwm_opay_merchant_acquiring_merchant_di',
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
dependence_dwd_opay_merchant_acquiring_record_di_prev_day_task = UFileSensor(
    task_id='dependence_dwd_opay_merchant_acquiring_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_merchant_acquiring_record_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwm_opay_merchant_acquiring_merchant_di"
hdfs_path = "ufile://opay-datalake/opay/opay_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dwm_opay_merchant_acquiring_merchant_di_task = HiveOperator(

    task_id='dwm_opay_merchant_acquiring_merchant_di_task',
    hql='''
    
     set hive.exec.dynamic.partition.mode=nonstrict;
    INSERT overwrite TABLE opay_dw.{table} partition(country_code,dt)
    select merchant_id,'MAcquiring' service_type,order_status,sum(amount) s_amount,count(1) c,country,dt
     from dwd_opay_merchant_acquiring_record_di where dt='{pt}'
    group by merchant_id,order_status,country,dt;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='opay_dw',
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

dependence_dwd_opay_merchant_acquiring_record_di_prev_day_task >> \
sleep_time >> \
dwm_opay_merchant_acquiring_merchant_di_task >> \
touchz_data_success
