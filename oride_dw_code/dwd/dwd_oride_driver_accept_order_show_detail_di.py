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
    'owner': 'linan',
    'start_date': datetime(2019, 5, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_driver_accept_order_show_detail_di',
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
dwd_oride_driver_accept_order_show_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
        pt='{{ds}}',
        hour='23'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_driver_accept_order_show_detail_di"
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

dwd_oride_driver_accept_order_show_detail_di_task = HiveOperator(

    task_id='dwd_oride_driver_accept_order_show_detail_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    SELECT driver_id,
           --主键（driver_id,order_id）

           city_id,
           --定位城市

           product_id,
           --接单司机类型，1=专车，2=快车

           order_id,
           --订单号

           startLat,
           --订单当前纬度

           startLng,
           --订单当前经度

           lat,
           --用户当前纬度

           lng,
           --用户当前经度

           log_timestamp,
           --埋点时间
           isAssign, --是否强派单,1强派单,0非强派单
           event_name,  --事件类型

           'nal' AS country_code,
           --国家码字段

           '{pt}' AS dt
    FROM
      (SELECT 
              user_id AS driver_id,
              get_json_object(event_value, '$.order_id') AS order_id,
              get_json_object(event_value, '$.city_id') AS city_id,
              get_json_object(event_value, '$.serv_type') AS product_id,
              get_json_object(event_value, '$.lat') AS lat,
              get_json_object(event_value, '$.lng') AS lng,
              get_json_object(event_value, '$.startLat') AS startLat,
              get_json_object(event_value, '$.startLng') AS startLng,
              event_time AS log_timestamp,
              get_json_object(event_value, '$.isAssign') AS isAssign,  --是否强派单
              event_name  --事件类型
       FROM oride_dw.dwd_oride_client_event_detail_hi
       WHERE dt='{pt}'
         AND event_name in('accept_order_show','order_push_show')
         union all
         SELECT 
              user_id AS driver_id,
              order_id,
              get_json_object(event_value, '$.city_id') AS city_id,
              get_json_object(event_value, '$.serv_type') AS product_id,
              get_json_object(event_value, '$.lat') AS lat,
              get_json_object(event_value, '$.lng') AS lng,
              get_json_object(event_value, '$.startLat') AS startLat,
              get_json_object(event_value, '$.startLng') AS startLng,
              event_time AS log_timestamp,
              get_json_object(event_value, '$.isAssign') AS isAssign,  --是否强派单
              event_name  --事件类型
       FROM oride_dw.dwd_oride_client_event_detail_hi
       lateral view explode(split(substr(get_json_object(event_value, '$.order_ids'),2,length(get_json_object(event_value, '$.order_ids'))-2),',')) order_ids as order_id
       WHERE dt='{pt}'
         AND event_name in('accept_order_show','order_push_show')
       ) t 
       ;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    schema='oride_dw',
    dag=dag)


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

dwd_oride_driver_accept_order_show_detail_di_prev_day_task >> \
sleep_time >> \
dwd_oride_driver_accept_order_show_detail_di_task >> \
touchz_data_success
