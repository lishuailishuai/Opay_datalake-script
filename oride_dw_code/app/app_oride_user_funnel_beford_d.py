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
from airflow.sensors.s3_key_sensor import S3KeySensor
import json
import logging
from airflow.models import Variable
import requests
import os
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_user_funnel_beford_d',
                  schedule_interval="40 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "oride_dw"
table_name = "app_oride_user_funnel_beford_d"
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
dwd_oride_order_base_include_test_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
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
            "dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
            "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"
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

def app_oride_user_funnel_beford_d_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    set hive.mapred.mode=nonstrict;
    
    with base as 
    (select user_id,
            event_name,
            event_time,
    				if(event_name='oride_show',1,if(event_name='choose_end_point_click',2,3)) as rn
    from oride_dw.dwd_oride_client_event_detail_hi 
    where dt='{pt}'
          and from_unixtime(cast(event_time as int),'yyyy-MM-dd')=dt
          and app_version>='4.4.405' 
          and event_name in('oride_show','choose_end_point_click','request_a_ride_show')
    group by user_id,event_name,event_time
    )

    insert overwrite table {db}.{table} partition(country_code,dt)
    select sum(t.active_user_num) as active_user_num,--活跃乘客数
           sum(t.check_end_user_num) as check_end_user_num,--选择终点乘客数量
           sum(t.arrive_valuation_user_num) as arrive_valuation_user_num, --到达估价页面乘客量
           sum(t.check_end_cnt) as check_end_cnt,--选择终点次数
           sum(t.valuation_cnt) as valuation_cnt,--估价次数
           sum(t.login_to_check_end_dur) as login_to_check_end_dur,--登录到选择终点平均时长
           sum(t.check_end_to_valuation_dur) as check_end_to_valuation_dur,--选择终点到预估价格平均时长
           sum(t.login_to_valuation_dur) as login_to_valuation_dur,--登录到预估价格平均时长
           sum(t.valuation_to_order_dur) as valuation_to_order_dur,--估价界面到下单平均时长
           'nal' as country_code,
           '{pt}' as dt    
    from (select count(distinct if(event_name='oride_show',user_id,null))as active_user_num,--活跃乘客数
           count(distinct if(event_name='choose_end_point_click',user_id,null)) as check_end_user_num,--选择终点乘客数量
           sum(if(event_name='choose_end_point_click',1,null)) as check_end_cnt,--选择终点次数
           count(distinct if(event_name='request_a_ride_show',user_id,null)) as arrive_valuation_user_num, --到达估价页面乘客量
           sum(if(event_name='request_a_ride_show',1,null)) as valuation_cnt,--估价次数
           null as login_to_check_end_dur,--登录到选择终点平均时长
           null as check_end_to_valuation_dur,--选择终点到预估价格平均时长
           null as login_to_valuation_dur,--登录到预估价格平均时长
           null as valuation_to_order_dur--估价界面到下单平均时长
    from base
    union all
    --以下统计乘客行为类指标    
    SELECT null as active_user_num,--活跃乘客数
           null as check_end_user_num,--选择终点乘客数量
           null as check_end_cnt,--选择终点次数
           null as arrive_valuation_user_num, --到达估价页面乘客量
           null as valuation_cnt,--估价次数
                 avg(if(event_name_a='choose_end_point_click' and event_name_b='oride_show' and (time_range>0 and time_range < 15*60),time_range,0)) as login_to_check_end_dur,--登录到选择终点平均时长
           avg(if(event_name_a='request_a_ride_show' and event_name_b='choose_end_point_click' and (time_range>0 and time_range < 15*60),time_range,0)) as check_end_to_valuation_dur,--选择终点到预估价格平均时长
           avg(if(event_name_a='request_a_ride_show' and event_name_b='oride_show' and (time_range>0 and time_range < 15*60),time_range,0)) as login_to_valuation_dur,--登录到预估价格平均时长
           null as valuation_to_order_dur--估价界面到下单平均时长
    FROM
      (SELECT a.user_id AS user_id_a,
              a.event_name AS event_name_a,
              a.event_time AS event_time_a,
              b.user_id AS user_id_b,
              b.event_name AS event_name_b,
              b.event_time AS event_time_b,
              (a.event_time-b.event_time) as time_range,
              row_number() over (partition BY a.user_id,a.event_name,b.event_name
                                 ORDER BY a.event_time desc,b.event_time desc) AS rrn
       FROM base a
       inner JOIN base b ON a.user_id=b.user_id
       AND a.event_time>b.event_time
       and a.rn>b.rn) m
    WHERE m.rrn=1 
    union all
    --以下统计预估价格界面到下单平均时长
    select null as active_user_num,--活跃乘客数
           null as check_end_user_num,--选择终点乘客数量
           null as check_end_cnt,--选择终点次数
           null as arrive_valuation_user_num, --到达估价页面乘客量
           null as valuation_cnt,--估价次数
           null as login_to_check_end_dur,--登录到选择终点平均时长
           null as check_end_to_valuation_dur,--选择终点到预估价格平均时长
           null as login_to_valuation_dur,--登录到预估价格平均时长
           avg(if(time_range>0 and time_range < 15*60,time_range,0)) as valuation_to_order_dur--估价界面到下单平均时长
    from (select ord.passenger_id,
                 ord.create_time,
                 t.user_id,
                 t.event_name,
                 t.event_time,
                 (ord.create_time-t.event_time) as time_range,
                 row_number() over (partition BY ord.passenger_id
                                         ORDER BY ord.create_time desc,t.event_time desc) AS rrn
            from (select passenger_id,create_time
            from oride_dw.dwd_oride_order_base_include_test_di
            where dt='{pt}' 
            group by passenger_id,create_time) ord 
            inner join
            (select * from base where event_name='request_a_ride_show') t 
            on ord.passenger_id=t.user_id
            and ord.create_time>t.event_time) n
            where n.rrn=1) t;
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
    _sql = app_oride_user_funnel_beford_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_user_funnel_beford_d_task = PythonOperator(
    task_id='app_oride_user_funnel_beford_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_client_event_detail_hi_task >> app_oride_user_funnel_beford_d_task
dwd_oride_order_base_include_test_di_prev_day_task >> app_oride_user_funnel_beford_d_task