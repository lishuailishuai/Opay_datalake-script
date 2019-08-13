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
        'owner': 'yangmingze',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwm_oride_order_pay_finish_di', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 

#依赖前一天分区
dwd_oride_order_pay_detail_di_prev_day_tesk=UFileSensor(
    task_id='dwd_oride_order_pay_detail_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_pay_detail_di/country_code=nal",
        pt='{{ds}}'
        ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
    dag=dag
        )

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwm_oride_order_pay_finish_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 

dwm_oride_order_pay_finish_di_task = HiveOperator(

    task_id='dwm_oride_order_pay_finish_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

SELECT order_id,
       --订单 ID

       product_id,
       --订单业务类型(0: all 1:driect 2: street)

       city_id,
       --所属城市

       driver_id,
       --司机ID

       passenger_id,
       --乘客 ID

       count(1) AS finish_pay_cnt,
       --支付成功的订单数

       SUM(price) AS pay_price_total,
       --订单完成后总金额

       SUM(pay_amount) AS pay_amount_total,
       --完成支付的实际金额

       sum(CASE WHEN (pay_mode=2
                        OR pay_mode=3) THEN 1 ELSE 0 END) AS online_pay_order_cnt,
                         --在线支付订单数

       country_code,
       dt

FROM oride_dw.dwd_oride_order_pay_detail_di
WHERE dt='{pt}'
  AND status=1
  GROUP BY order_id,
            product_id,
         driver_id,
         city_id,
         passenger_id,
         country_code,
         dt
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
    dag=dag)

#生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`
    
    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path+'/country_code=nal/dt={{ds}}'
        ),
    dag=dag)


dwd_oride_order_pay_detail_di_prev_day_tesk>>sleep_time>>dwm_oride_order_pay_finish_di_task>>touchz_data_success
