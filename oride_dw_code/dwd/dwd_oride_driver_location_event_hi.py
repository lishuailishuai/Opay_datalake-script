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

dag = airflow.DAG('dwd_oride_driver_location_event_hi',
                  schedule_interval="40 * * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一小时分区
dwd_oride_client_event_detail_hi_hour_task = UFileSensor(
    task_id='dwd_oride_client_event_detail_hi_hour_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_driver_location_event_hi"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, execution_date, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}/hour={hour}".format(pt=ds, hour=execution_date.strftime("%H")),
         "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_location_driver_event_hi_task = HiveOperator(
    task_id='dwd_oride_location_driver_event_hi_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt,hour)

        select 
        t.order_id, --订单id
        t.driver_id, --司机id
        
        replace(concat_ws(',',collect_set(if(accept_order_click_lat is null,'',accept_order_click_lat))),',','') as accept_order_click_lat, --accept_order_click，事件，纬度
        replace(concat_ws(',',collect_set(if(accept_order_click_lng is null,'',accept_order_click_lng))),',','') as accept_order_click_lng, --accept_order_click，事件，经度
        replace(concat_ws(',',collect_set(if(confirm_arrive_click_arrived_lat is null,'',confirm_arrive_click_arrived_lat))),',','') as confirm_arrive_click_arrived_lat, --confirm_arrive_click_arrived，事件，纬度
        replace(concat_ws(',',collect_set(if(confirm_arrive_click_arrived_lng is null,'',confirm_arrive_click_arrived_lng))),',','') as confirm_arrive_click_arrived_lng, --confirm_arrive_click_arrived，事件，经度
        replace(concat_ws(',',collect_set(if(pick_up_passengers_sliding_arrived_lat is null,'',pick_up_passengers_sliding_arrived_lat))),',','') as pick_up_passengers_sliding_arrived_lat, --pick_up_passengers_sliding_arrived，事件，纬度
        replace(concat_ws(',',collect_set(if(pick_up_passengers_sliding_arrived_lng is null,'',pick_up_passengers_sliding_arrived_lng))),',','') as pick_up_passengers_sliding_arrived_lng, --pick_up_passengers_sliding_arrived，事件，经度
        replace(concat_ws(',',collect_set(if(start_ride_sliding_lat is null,'',start_ride_sliding_lat))),',','') as start_ride_sliding_lat, --start_ride_sliding，事件，纬度
        replace(concat_ws(',',collect_set(if(start_ride_sliding_lng is null,'',start_ride_sliding_lng))),',','') as start_ride_sliding_lng, --start_ride_sliding，事件，经度
        replace(concat_ws(',',collect_set(if(start_ride_sliding_arrived_lat is null,'',start_ride_sliding_arrived_lat))),',','') as start_ride_sliding_arrived_lat, --start_ride_sliding_arrived，事件，纬度
        replace(concat_ws(',',collect_set(if(start_ride_sliding_arrived_lng is null,'',start_ride_sliding_arrived_lng))),',','') as start_ride_sliding_arrived_lng, --start_ride_sliding_arrived，事件，经度
        
        'nal' as country_code,
        '{now_day}' as dt,
        '{now_hour}' as hour
        
        
        from 
        (	
            select
            get_json_object(event_value,'$.order_id') order_id,
            user_id as driver_id,
            if(event_name = 'accept_order_click',get_json_object(event_value,'$.lat'),null) as accept_order_click_lat,
            if(event_name = 'accept_order_click',get_json_object(event_value,'$.lng'),null) as accept_order_click_lng,
            if(event_name = 'confirm_arrive_click_arrived',get_json_object(event_value,'$.lat'),null) as confirm_arrive_click_arrived_lat,
            if(event_name = 'confirm_arrive_click_arrived',get_json_object(event_value,'$.lng'),null) as confirm_arrive_click_arrived_lng,
            if(event_name = 'pick_up_passengers_sliding_arrived',get_json_object(event_value,'$.lat'),null) as pick_up_passengers_sliding_arrived_lat,
            if(event_name = 'pick_up_passengers_sliding_arrived',get_json_object(event_value,'$.lng'),null) as pick_up_passengers_sliding_arrived_lng,
            if(event_name = 'start_ride_sliding',get_json_object(event_value,'$.lat'),null) as start_ride_sliding_lat,
            if(event_name = 'start_ride_sliding',get_json_object(event_value,'$.lng'),null) as start_ride_sliding_lng,
            if(event_name = 'start_ride_sliding_arrived',get_json_object(event_value,'$.lat'),null) as start_ride_sliding_arrived_lat,
            if(event_name = 'start_ride_sliding_arrived',get_json_object(event_value,'$.lng'),null) as start_ride_sliding_arrived_lng
                    
        
            from oride_dw.dwd_oride_client_event_detail_hi
            where dt = '{now_day}'
            and hour = '{now_hour}'
            and event_name in (
                'accept_order_click',
                'confirm_arrive_click_arrived',
                'pick_up_passengers_sliding_arrived',
                'start_ride_sliding',
                'start_ride_sliding_arrived'
            )
        ) t 
        where t.order_id is not null
        group by 
        t.order_id,
        t.driver_id
        
        ;


'''.format(
        pt='{{ds}}',
        now_day='{{ds}}',
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name
    ),
    dag=dag
)


def check_key_data(ds, execution_date, **kargs):
    # 主键重复校验
    HQL_DQC = '''
    SELECT count(1) as nm
    FROM
     (SELECT order_id,
             driver_id,
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      and hour = '{now_hour}'
      GROUP BY 
      order_id,
      driver_id 
      HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=ds,
        now_hour=execution_date.strftime("%H"),
        table=table_name
    )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] > 1:
        raise Exception("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")


# 主键重复校验
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag)


# 生成_SUCCESS
def check_success(ds, execution_date, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"table": "{dag_name}".format(dag_name=dag_ids),
         "hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}/hour={hour}".format(pt=ds,
                                                                               hour=execution_date.strftime("%H"),
                                                                               hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)


touchz_data_success = PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dwd_oride_client_event_detail_hi_hour_task >> sleep_time >> dwd_oride_location_driver_event_hi_task >> task_check_key_data >> touchz_data_success
