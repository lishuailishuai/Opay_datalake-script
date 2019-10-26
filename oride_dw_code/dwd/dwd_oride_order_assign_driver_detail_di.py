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
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess

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

dag = airflow.DAG('dwd_oride_order_assign_driver_detail_di',
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
dependence_dwd_oride_order_assign_driver_detail_di_prev_day_task = HivePartitionSensor(
    task_id="dwd_oride_order_assign_driver_detail_di_prev_day_task",
    table="dispatch_tracker_server_magic",
    partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_order_assign_driver_detail_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name



##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_order_assign_driver_detail_di_task = HiveOperator(
    task_id='dwd_oride_order_assign_driver_detail_di_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt)

        select 
        d.city_id ,--下单时所在城市
        d.order_id, --订单ID
        d.passenger_id,--乘客ID
        d.driver_id,--司机ID
        d.order_round,--订单轮数
        d.config_id,--派单配置的id
        d.success,--是否成功（1，0）
        d.log_timestamp,--埋点时间
        d.dis,--司机的接驾距离(米)(订单分配给司机时司机所处的位置)(预估)
        d.wait_time,--司机收到推送信息时有多久没有订单
        d.push_mode,--是派单方式（目前只有全局优化和直接发单）
        'nal' as country_code, 
        '{pt}' dt

        from 
        (
            select
            *
            from 
            (
                select 
                split(replace(replace(get_json_object(event_values, '$.driver_ids'),'[',''),']',''), ',') as drivers, 
                split(replace(replace(get_json_object(event_values, '$.distances'),'[',''),']',''), ',') as distances,
                get_json_object(event_values, '$.order_id') as order_id,
                get_json_object(event_values, '$.city_id') as city_id,
                get_json_object(event_values, '$.user_id') as passenger_id,
                get_json_object(event_values, '$.round') as order_round,
                get_json_object(event_values, '$.config_id') as config_id,
                get_json_object(event_values, '$.success') as success,
                get_json_object(event_values, '$.timestamp') as log_timestamp,
                get_json_object(event_values, '$.wait_time') as wait_time,
                get_json_object(event_values, '$.mode') as push_mode

                from oride_source.dispatch_tracker_server_magic 
                where dt='{pt}' and 
                event_name='dispatch_assign_driver' and 
                from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd')='{pt}'
            ) as t 
            lateral view posexplode(drivers) d as dpos, driver_id 
            lateral view posexplode(distances) ds as dspos, dis 
            where dpos = dspos
        ) d



'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    dag=dag
)

# 生成_SUCCESS
def check_success(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"table": "{dag_name}".format(dag_name=dag_ids),
         "hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds, hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success = PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_order_assign_driver_detail_di_prev_day_task >> sleep_time >> dwd_oride_order_assign_driver_detail_di_task >> touchz_data_success
