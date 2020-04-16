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
from plugins.CountriesAppFrame import CountriesAppFrame
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors import OssSensor
from plugins.CountriesPublicFrame import CountriesPublicFrame

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 10),
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
    (select * from (select *,
            lag(rn,1,0) over(partition by user_id,platform,app_version,tid order by event_time) as rn_lag,
            max(rn) over(partition by user_id,platform,app_version,tid) as rn_max,
            sum(if(rn-lag(rn,1,0) over(partition by user_id,platform,app_version,tid order by event_time)=1,1,0)) over(partition by user_id,platform,app_version,tid) as rr
    from (SELECT user_id,  --乘客ID
           platform,  --操作系统
           app_version, --app版本号
           if(event_name in('choose_end_point_click','recommended_destination_click'),'choose_end_point_click',event_name) as event_name,
           cast(substr(event_time,1,10) AS bigint) AS event_time,
           tid,
           CASE
               WHEN event_name='oride_show' THEN 1 --登录页面
    
               WHEN event_name in('choose_end_point_click','recommended_destination_click') THEN 2 --选择终点
    
               WHEN event_name='request_a_ride_show' THEN 3 --估价页面
    
               WHEN event_name='success_request_a_ride' THEN 4 --下单页面
    
               WHEN event_name='successful_order_show' THEN 5 --接单节点
    
               WHEN event_name='complete_the_order_show' THEN 6 --完单节点
    
               ELSE 7
           END AS rn, --完成支付节点
           row_number() over (partition by user_id,platform,app_version,event_name,tid order by event_time desc) as rn_time 
    
    FROM oride_dw.dwd_oride_client_event_detail_hi
    WHERE dt='{pt}'
      AND from_unixtime(cast(substr(event_time,1,10) AS bigint),'yyyy-MM-dd')=dt 
      and tid is not null
    
      AND (event_name IN('choose_end_point_click',
                         'recommended_destination_click',
                         'request_a_ride_show',
                         'success_request_a_ride',
                         'successful_order_show',
                         'complete_the_order_show',
                         'complete_the_payment_show')
           OR (event_name='oride_show'
               AND lower(app_name) IN('oride passenger',
                                      'oride',
                                      'opay')))    
    ) t where t.rn_time=1) m  
    where (m.rr=m.rn_max) or (m.rr<>m.rn_max and rn=1) )  --只取以登录开始的连续event，如果中间断流，将过滤掉
    
    insert overwrite table {db}.{table} partition(country_code,dt)
    select nvl(city_id,-10000) as city_id, --城市ID
           nvl(product_id,-10000) as product_id, --业务线ID
           nvl(user_version_os,'-10000') as user_version_os, --乘客端版本号和操作系统
           active_user_num_m,--活跃乘客数
           check_end_user_num_m,--选择终点乘客数量
           arrive_valuation_user_num_m, --到达估价页面乘客量
           ord_user_num_m, --下单乘客数-m
           request_user_num_m,--被接单乘客数量
           finish_user_num_m,--完单乘客数量
           finished_pay_user_num_m,--完单支付乘客数量
           login_cnt_m, --登录次数
           chose_end_cnt_m,--选择终点次数
           valuation_cnt_m,--估价次数
           login_to_check_end_dur,--登录到选择终点平均时长
           check_end_to_valuation_dur,--选择终点到预估价格平均时长
           login_to_valuation_dur,--登录到预估价格平均时长
           valuation_to_order_dur,--估价界面到下单平均时长
           'nal' as country_code,
		   '{pt}' as dt
    from (SELECT -10000 as city_id, --城市ID
           -10000 as product_id, --业务线ID
             concat_ws('_',app_version,platform) AS user_version_os, --乘客端版本号和操作系统
			 count(DISTINCT (if(event_name_a='oride_show',user_id_a,NULL))) AS active_user_num_m,--活跃乘客数
			 count(DISTINCT (if(event_name_a='choose_end_point_click'
			                    AND event_name_b='oride_show',user_id_a,NULL))) AS check_end_user_num_m,--选择终点乘客数量
			 count(DISTINCT (if(event_name_a='request_a_ride_show'
			                    AND event_name_b='choose_end_point_click',user_id_a,NULL))) AS arrive_valuation_user_num_m, --到达估价页面乘客量
			 count(DISTINCT (if(event_name_a='success_request_a_ride'
			                    AND event_name_b='request_a_ride_show',user_id_a,NULL))) AS ord_user_num_m, --下单乘客数-m
			 count(DISTINCT (if(event_name_a='successful_order_show'
			                    AND event_name_b='success_request_a_ride',user_id_a,NULL))) AS request_user_num_m,--被接单乘客数量
			 count(DISTINCT (if(event_name_a='complete_the_order_show'
			                    AND event_name_b='successful_order_show',user_id_a,NULL))) AS finish_user_num_m,--完单乘客数量
			 count(DISTINCT (if(event_name_a='complete_the_payment_show'
			                    AND event_name_b='complete_the_order_show',user_id_a,NULL))) AS finished_pay_user_num_m,--完单支付乘客数量
			 sum(if(event_name_a='oride_show',1,0)) as login_cnt_m, --登录次数
			 sum((if(event_name_a='choose_end_point_click'
			         AND event_name_b='oride_show',1,0))) AS chose_end_cnt_m,--选择终点次数
			 sum((if(event_name_a='request_a_ride_show'
			         AND event_name_b='choose_end_point_click',1,0))) AS valuation_cnt_m,--估价次数
			 avg(if(event_name_a='choose_end_point_click'
			        AND event_name_b='oride_show'
			        AND (time_range>0
			             AND time_range < 15*60),time_range,null)) AS login_to_check_end_dur,--登录到选择终点平均时长
			 avg(if(event_name_a='request_a_ride_show'
			        AND event_name_b='choose_end_point_click'
			        AND (time_range>0
			             AND time_range < 15*60),time_range,null)) AS check_end_to_valuation_dur,--选择终点到预估价格平均时长
			 avg(if(event_name_a='request_a_ride_show'
			        AND event_name_b='oride_show'
			        AND (time_range>0
			             AND time_range < 15*60),time_range,null)) AS login_to_valuation_dur,--登录到预估价格平均时长
			 avg(if(event_name_a='success_request_a_ride'
			                    AND event_name_b='request_a_ride_show'
			                    and (time_range>0
			             AND time_range < 15*60),time_range,NULL)) AS valuation_to_order_dur--估价界面到下单平均时长
			
			FROM
			  (SELECT a.tid AS tid_a,
			          a.user_id AS user_id_a,
			          a.event_name AS event_name_a,
			          a.event_time AS event_time_a,
			          b.tid AS tid_b,
			          b.user_id AS user_id_b,
			          b.event_name AS event_name_b,
			          b.event_time AS event_time_b,
			          a.app_version,
			          a.platform,
			          (a.event_time-b.event_time) AS time_range
			   FROM base a
			   LEFT JOIN base b ON a.tid=b.tid
			   AND a.event_time>b.event_time
			   AND a.rn>b.rn
			   ) m
			GROUP BY concat_ws('_',app_version,platform)
			with cube) t;
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL

#主流程
def execution_data_task_id(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_user_funnel_beford_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

app_oride_user_funnel_beford_d_task = PythonOperator(
    task_id='app_oride_user_funnel_beford_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwd_oride_client_event_detail_hi_task >> app_oride_user_funnel_beford_d_task