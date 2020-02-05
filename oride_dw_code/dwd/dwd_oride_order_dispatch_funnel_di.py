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
    'start_date': datetime(2019, 9, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_dispatch_funnel_di',
                  schedule_interval="40 00 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)
##----------------------------------------- 变量 ---------------------------------------##

table_name = "dwd_oride_order_dispatch_funnel_di"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    # 依赖前一天分区
    dependence_dispatch_tracker_server_magic_prev_day_task = HivePartitionSensor(
        task_id="dependence_dispatch_tracker_server_magic_prev_day_task",
        table="dispatch_tracker_server_magic",
        partition="dt='{{ ds }}' and hour='23'",
        schema="oride_source",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:
    print("成功")
    dependence_dispatch_tracker_server_magic_prev_day_task = HivePartitionSensor(
        task_id="dependence_dispatch_tracker_server_magic_prev_day_task",
        table="dispatch_tracker_server_magic",
        partition="dt='{{ ds }}' and hour='23'",
        schema="oride_source",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "5400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_order_dispatch_funnel_di_task = HiveOperator(
    task_id='dwd_oride_order_dispatch_funnel_di_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        with order_dispatch as (
            select 
            event_name,
            event_values
            from 
            oride_source.dispatch_tracker_server_magic 
            where dt = '{pt}'
            and event_name in ('dispatch_chose_driver','dispatch_filter_driver','dispatch_assign_driver','dispatch_push_driver')
        )
        
        
        insert overwrite table oride_dw.dwd_oride_order_dispatch_funnel_di partition(country_code,dt)
        
        select 
            cast(get_json_object(event_values, '$.city_id') as bigint) as city_id,--下单时所在城市
            cast(get_json_object(event_values, '$.order_id') as bigint) as order_id, --订单ID
            cast(get_json_object(event_values, '$.user_id') as bigint) as passenger_id, --乘客ID
            cast(driver_id as bigint) as driver_id,--司机ID
            cast(get_json_object(event_values, '$.round') as bigint) as order_round,--订单轮数
            cast(get_json_object(event_values, '$.config_id') as bigint) as config_id,--派单配置的id
            0 as success,--是否成功（1，0）                                                                                            
            cast(get_json_object(event_values, '$.timestamp') as string) as log_timestamp,--埋点时间
            0 as distance,--司机的接驾距离(米)(订单分配给司机时司机所处的位置)
            '' as wait_time,--司机收到推送信息时有多久没有订单
            '' as push_mode,--是派单方式（目前只有全局优化和直接发单）
            '' as reason,--过滤原因
            event_name as event_name, --事件类型
            0 as assign_type, --0=非强派单，1=强派单
            cast(get_json_object(event_values, '$.serv_type') as int) as product_id, --业务线ID
            'nal' as country_code,
            '{pt}' dt
        from  
        order_dispatch 
        lateral view explode(split(substr(get_json_object(event_values, '$.driver_ids'),2,length(get_json_object(event_values, '$.driver_ids'))-2),',')) driver_ids as driver_id
        where event_name='dispatch_chose_driver'  --圈选订单范围内的司机
        
        union all
        
        select 
            cast(get_json_object(event_values, '$.city_id') as bigint) as city_id,--下单时所在城市
            cast(get_json_object(event_values, '$.order_id') as bigint) as order_id, --订单ID
            cast(get_json_object(event_values, '$.user_id') as bigint) as passenger_id, --乘客ID
            cast(get_json_object(event_values, '$.driver_id') as bigint) as driver_id,--司机ID
            cast(get_json_object(event_values, '$.round') as bigint) as order_round,--订单轮数
            cast(get_json_object(event_values, '$.config_id') as bigint) as config_id,--派单配置的id
            0 as success,--是否成功（1，0）
            cast(get_json_object(event_values, '$.timestamp') as string) as log_timestamp,--埋点时间
            0 as distance,--司机的接驾距离(米)(订单分配给司机时司机所处的位置)
            '' as wait_time,--司机收到推送信息时有多久没有订单
            '' as push_mode,--是派单方式（目前只有全局优化和直接发单）
            get_json_object(event_values, '$.reason') as reason,--过滤原因
            event_name as event_name, --事件类型
            0 as assign_type, --0=非强派单，1=强派单
            cast(get_json_object(event_values, '$.serv_type') as int) as product_id, --业务线ID
            'nal' as country_code,
            '{pt}' dt
        from  
        order_dispatch 
        where  event_name='dispatch_filter_driver' --过滤未派单的司机，并标明被过滤的原因
        
        union all
        
        select 
            cast(d.city_id as bigint) as city_id,--下单时所在城市
            cast(d.order_id as bigint) as order_id, --订单ID
            cast(d.passenger_id as bigint) as passenger_id,--乘客ID
            cast(d.driver_id as bigint) as driver_id,--司机ID
            cast(d.order_round as bigint) as order_round,--订单轮数
            cast(d.config_id as bigint) as config_id,--派单配置的id
            cast(nvl(d.success,0) as bigint) as success,--是否成功（1，0）
            cast(d.log_timestamp as string) as log_timestamp,--埋点时间
            cast(nvl(d.distance,0) as bigint) as distance,--司机的接驾距离(米)(订单分配给司机时司机所处的位置)
            cast(d.wait_time as string) as wait_time,--司机收到推送信息时有多久没有订单
            cast(d.push_mode as string) as push_mode,--是派单方式（目前只有全局优化和直接发单）
            '' as reason,--过滤原因
            d.event_name as event_name, --事件类型
            0 as assign_type,--0=非强派单，1=强派单
            cast(d.product_id as int) as product_id, --业务线ID
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
                get_json_object(event_values, '$.mode') as push_mode,
                get_json_object(event_values, '$.serv_type') as product_id, --业务线ID
                event_name as event_name
        
                from order_dispatch
                where 
                event_name='dispatch_assign_driver' --订单分配给司机的过程
                and from_unixtime(cast(get_json_object(event_values, '$.timestamp') as int), 'yyyy-MM-dd')='{pt}'
            ) as t 
            lateral view posexplode(drivers) d as dpos, driver_id 
            lateral view posexplode(distances) ds as dspos, distance 
            where dpos = dspos  
        ) d
        
        union all
        
        select 
            cast(get_json_object(event_values, '$.city_id') as bigint) as city_id,--下单时所在城市
            cast(get_json_object(event_values, '$.order_id') as bigint) as order_id, --订单ID
            cast(get_json_object(event_values, '$.user_id') as bigint) as passenger_id, --乘客ID
            cast(get_json_object(event_values, '$.driver_id') as bigint) as driver_id,--司机ID
            cast(get_json_object(event_values, '$.round') as bigint) as order_round,--订单轮数
            cast(get_json_object(event_values, '$.config_id') as bigint) as config_id,--派单配置的id
            cast(nvl(get_json_object(event_values, '$.success'),0) as bigint) as success,--是否成功（1，0）
            cast(get_json_object(event_values, '$.timestamp') as string) as log_timestamp,--埋点时间
            cast(nvl(get_json_object(event_values, '$.distance'),0) as bigint) as distance,--司机的接驾距离(米)(订单播给司机时司机所处的位置)
            cast(get_json_object(event_values, '$.wait_time') as string) as wait_time,--司机收到推送信息时有多久没有订单
            cast(get_json_object(event_values, '$.mode') as string) as push_mode, --是派单方式（目前只有全局优化和直接发单）
            '' as reason,--过滤原因
            event_name as event_name, --事件类型
            cast(get_json_object(event_values, '$.assign_type') as bigint) as assign_type, --0=非强派单，1=强派单
            cast(get_json_object(event_values, '$.serv_type') as int) as product_id, --业务线ID
            'nal' as country_code,
            '{pt}' dt
        from
            order_dispatch 
        where  event_name='dispatch_push_driver'   --订单成功发送给司机的过程
        
        ;
        

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

dependence_dispatch_tracker_server_magic_prev_day_task >> sleep_time >> dwd_oride_order_dispatch_funnel_di_task >> touchz_data_success
