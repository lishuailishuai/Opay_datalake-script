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
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors import OssSensor

args = {
    'owner': 'chenghui',
    'start_date': datetime(2019, 11, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_passenger_funnel_limited_edition_d',
                  schedule_interval="40 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "oride_dw"
table_name = "app_oride_passenger_funnel_limited_edition_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dwd_oride_client_event_detail_hi_task = OssSensor(
        task_id="dwd_oride_client_event_detail_hi_task",
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

# ----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {
            "dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
            "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"
        }
    ]
    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_passenger_funnel_limited_edition_d_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;

    with base as 
    (select user_id,event_name,event_value,country_code,event_time,
        get_json_object(event_value, '$.serv_type') AS product_id,
        get_json_object(event_value, '$.order_id') AS order_id
    from oride_dw.dwd_oride_client_event_detail_hi 
    where from_unixtime(cast(event_time as int),'yyyy-MM-dd')='{pt}' 
        and dt='{pt}'  and app_version>='4.4.405' 
        and event_name in('oride_show','choose_end_point_click','request_a_ride_show')
    )
        insert overwrite table {db}.{table} partition(country_code,dt)
        select a.region_name,
            a.active_user_num,--活跃乘客数
            a.check_end_user_num,--选择终点乘客数量
            a.valuation_cnt,--估价次数
            a.arrive_valuation_user_num,--到达估价页面乘客量
            0 as driver_user_find_each_other_dur,--司乘互找时长
            b.login_valuation_dur,--登陆到估价时长
            b.check_end_to_valuation_dur,--选择终点到估价时长
            'nal' as country_code,
            '{pt}' as dt
        from 
        (    
            select country_code as region_name,
                count(distinct if(event_name='oride_show',user_id,null))as active_user_num,--活跃乘客数
                count(distinct if(event_name='choose_end_point_click',user_id,null)) as check_end_user_num,--选择终点乘客数量
                count(if(event_name='request_a_ride_show',1,null)) as valuation_cnt,--估价次数
                count(distinct if(event_name='request_a_ride_show',user_id,null)) as arrive_valuation_user_num --到达估价页面乘客量 
            from 
            (
                select user_id,order_id,event_name,event_value,country_code
                from base
                where event_name in ('oride_show', 'choose_end_point_click','request_a_ride_show')
            ) as t
            group by country_code
        )as a
        left join
        (
            select country_code as region_name,
                avg(case when (gj_time-ac_time>0 and gj_time-ac_time < 15*60) then gj_time-ac_time end) as login_valuation_dur,--登陆到估价时长
                avg(case when (gj_time-zd_time>0 and gj_time-zd_time <15*60) then gj_time-zd_time end) as check_end_to_valuation_dur --选择终点到估价时长
            from 
            (
                select 
                country_code,
                max(case when event_name='oride_show' then event_time_b end) ac_time,
                max(case when event_name='choose_end_point_click' and line_numner=2 then event_time_b end) zd_time,
                max(case when event_name='request_a_ride_show' then event_time_b end) gj_time
                from(
                    select *,row_number() over(partition by order_id,event_name,event_time_b order by event_time_b desc) line_numner from(
                        select  h.user_id_h,h.order_id,h.event_time_a,i.user_id_i,i.event_name,i.country_code,i.event_time_b,count(1) from
                        (
                            select user_id as user_id_h,order_id,event_time as event_time_a
                            from base 
                            where order_id>0
                        )h
                        left join
                        (
                            select country_code,user_id as user_id_i,event_name,event_time as event_time_b
                            from base 
                            where event_name in('oride_show','choose_end_point_click','request_a_ride_show') 
                        )i
                        on h.user_id_h=i.user_id_i
                        where event_time_a>event_time_b
                        group by h.user_id_h,h.order_id,h.event_time_a,i.user_id_i,i.event_name,i.country_code,i.event_time_b
                    )a
                )as b 
                group by country_code,order_id
            ) as t2
            group by t2.country_code
        ) as b
        on a.region_name=b.region_name;
   
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_oride_passenger_funnel_limited_edition_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_passenger_funnel_limited_edition_d_task = PythonOperator(
    task_id='app_oride_passenger_funnel_limited_edition_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_client_event_detail_hi_task >> app_oride_passenger_funnel_limited_edition_d_task