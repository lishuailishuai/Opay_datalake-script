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
    'owner': 'lili.chen',
    'start_date': datetime(2019, 9, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_strong_base_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dm_oride_order_strong_base_cube_d_prev_day_task = UFileSensor(
    task_id='dm_oride_order_strong_base_cube_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_order_strong_base_cube_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
# 依赖前一天分区
dependence_dm_oride_driver_base_d_prev_day_task = UFileSensor(
    task_id='dm_oride_driver_base_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_base_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##

table_name = "app_oride_order_strong_base_cube_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

app_oride_order_strong_base_cube_d_task = HiveOperator(

    task_id='app_oride_order_strong_base_cube_d_task',
    hql='''set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
          SELECT nvl(a.city_id,-10000) as city_id,
                 nvl(a.product_id,-10000) as product_id,
                 sum(a.strong_dispatch_driver_cnt) as strong_dispatch_driver_cnt, --强派单司机数
                 sum(a.strong_dispatch_order_cnt) as strong_dispatch_order_cnt, --强派单调度推单数
                 sum(a.finished_driver_cnt) as finished_driver_cnt, --完单司机数 
                 sum(a.strong_finished_driver_cnt) as strong_finished_driver_cnt, --强派单完单司机数
                 sum(a.strong_finished_order_cnt) as strong_finished_order_cnt, --强派单完单数
                 sum(a.strong_finish_order_pick_up_dis) as strong_finish_order_pick_up_dis, --强派单完单接驾距离(米)
                 sum(a.strong_finish_order_pick_up_assigned_cnt) as strong_finish_order_pick_up_assigned_cnt, --强派单订单被分配次数（计算平均接驾距离使用）
                 sum(a.strong_user_cancel_order_cnt) as strong_user_cancel_order_cnt, --强派单乘客取消订单数
                 sum(a.strong_driver_cancel_order_cnt) as strong_driver_cancel_order_cnt, --强派单司机取消订单数
                 sum(a.strong_paid_order_cnt) as strong_paid_order_cnt, --强派单支付订单数
                 sum(a.strong_paid_price) as strong_paid_price,  --强派单应付金额
                 sum(a.strong_paid_amount) as strong_paid_amount,  --强派单实付金额
                 sum(b.finish_driver_online_dur) as finish_driver_online_dur, --当日完单司机在线时长
                 sum(b.strong_finish_driver_online_dur) as strong_finish_driver_online_dur, --当日强制派单完单司机在线时长
                 sum(a.push_show_ord_cnt) as push_show_ord_cnt, --push到达单数（派单）
                 sum(a.accept_show_ord_cnt) as accept_show_ord_cnt, --展示单数（派单）
                 sum(a.show_ord_cnt) as show_ord_cnt, --推送订单数（派单）
                 sum(a.accept_click_ord_cnt) as accept_click_ord_cnt, --接单数（派单）
                 'nal' as country_code,
                 '{pt}' as dt
FROM
  (SELECT *
   FROM oride_dw.dm_oride_order_strong_base_cube_d
   WHERE dt='{pt}') a
LEFT JOIN
  (SELECT nvl(country_code,'-10000') AS country_code,
          nvl(cast(city_id AS bigint),-10000) AS city_id,
          nvl(product_id,-10000) AS product_id,
          sum(finish_driver_online_dur) AS finish_driver_online_dur, --当日完单司机在线时长
 sum(strong_finish_driver_online_dur) AS strong_finish_driver_online_dur --当日强制派单完单司机在线时长
FROM oride_dw.dm_oride_driver_base_d
   WHERE dt='{pt}'
   GROUP BY nvl(country_code,'-10000'),
            nvl(cast(city_id AS bigint),-10000),
            nvl(product_id,-10000) WITH CUBE) b ON a.city_id=nvl(b.city_id,-10000)
AND a.product_id=nvl(b.product_id,-10000)
AND a.country_code=nvl(b.country_code,-10000)
group by nvl(a.city_id,-10000),nvl(a.product_id,-10000)
                 '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    dag=dag)

# 生成_SUCCESS
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
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

dependence_dm_oride_order_strong_base_cube_d_prev_day_task >> \
dependence_dm_oride_driver_base_d_prev_day_task >> \
sleep_time >> \
app_oride_order_strong_base_cube_d_task >> \
touchz_data_success


