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

dim_oride_passenger_base_prev_day_task = OssSensor(
    task_id='dim_oride_passenger_base_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_passenger_base/country_code=nal",
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
    (SELECT user_id,  --乘客ID
           user_number,  --乘客电话
           platform,  --操作系统
           app_version, --app版本号
           event_name,
           cast(substr(event_time,1,10) AS bigint) AS event_time,
           tid,
           CASE
               WHEN event_name='oride_show' THEN 1 --登录页面
    
               WHEN event_name='choose_end_point_click' THEN 2 --选择终点
    
               WHEN event_name='request_a_ride_show' THEN 3 --估价页面
    
               WHEN event_name='success_request_a_ride' THEN 4 --下单页面
    
               WHEN event_name='successful_order_show' THEN 5 --接单节点
    
               WHEN event_name='complete_the_order_show' THEN 6 --完单节点
    
               ELSE 7
           END AS rn --完成支付节点
    
    FROM oride_dw.dwd_oride_client_event_detail_hi
    WHERE dt='{pt}'
      AND from_unixtime(cast(substr(event_time,1,10) AS bigint),'yyyy-MM-dd')=dt 
    
      AND (event_name IN('choose_end_point_click',
                         'request_a_ride_show',
                         'success_request_a_ride',
                         'successful_order_show',
                         'complete_the_order_show',
                         'complete_the_payment_show')
           OR (event_name='oride_show'
               AND lower(app_name) IN('oride passenger',
                                      'oride',
                                      'opay')))
    GROUP BY user_id,
             user_number,
             platform,
             app_version,
             event_name,
             cast(substr(event_time,1,10) AS bigint),
             tid      
    )
    
    SELECT concat_ws('_',app_version,platform) AS user_version_os, --乘客端版本号和操作系统
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
			 sum((if(event_name_a='request_a_ride_show'
			         AND event_name_b='choose_end_point_click',1,0))) AS valuation_cnt_m,--估价次数
			 avg(if(event_name_a='choose_end_point_click'
			        AND event_name_b='oride_show'
			        AND (time_range>0
			             AND time_range < 15*60),time_range,0)) AS login_to_check_end_dur,--登录到选择终点平均时长
			 avg(if(event_name_a='request_a_ride_show'
			        AND event_name_b='choose_end_point_click'
			        AND (time_range>0
			             AND time_range < 15*60),time_range,0)) AS check_end_to_valuation_dur,--选择终点到预估价格平均时长
			 avg(if(event_name_a='request_a_ride_show'
			        AND event_name_b='oride_show'
			        AND (time_range>0
			             AND time_range < 15*60),time_range,0)) AS login_to_valuation_dur,--登录到预估价格平均时长
			 avg(if(event_name_a='request_a_ride_show'
			        AND phone_number IS NOT NULL
			        AND time_range1>0
			        AND time_range1 < 15*60,time_range1,0)) AS valuation_to_order_dur--估价界面到下单平均时长
			
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
			          (a.event_time-b.event_time) AS time_range,
			          (a.event_time-ord.create_time) AS time_range1, --估价到下单时间差
			 ord.phone_number
			   FROM base a
			   LEFT JOIN base b ON a.tid=b.tid
			   AND a.event_time>b.event_time
			   AND a.rn>b.rn
			   LEFT JOIN
			     (SELECT o.passenger_id,
			             o.create_time,
			             u.phone_number
			      FROM
			        (SELECT passenger_id,
			                create_time
			         FROM oride_dw.dwd_oride_order_base_include_test_di
			         WHERE dt='{pt}'
			         GROUP BY passenger_id,
			                  create_time) o
			      LEFT JOIN
			        (SELECT *
			         FROM oride_dw.dim_oride_passenger_base
			         WHERE dt='{pt}'
			           AND length(phone_number)=14) u 
			      ON o.passenger_id=u.passenger_id) ord 
			   ON (CASE WHEN a.event_name='request_a_ride_show' AND length(a.user_number) IN(10,14) 
			            THEN substr(a.user_number,-10)
			            END)=substr(ord.phone_number,-10)
			   AND a.event_time>ord.create_time) m
			GROUP BY concat_ws('_',app_version,platform);
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL

# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
        alter语句: alter_partition
        删除分区: delete_partition
        生产success: touchz_success

        #参数
        第一个参数true: 所有国家是否上线。false 没有
        第二个参数true: 数据目录是有country_code分区。false 没有
        第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """

    cf = CountriesPublicFrame("false", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_user_funnel_beford_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

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
dwd_oride_order_base_include_test_di_prev_day_task >> app_oride_user_funnel_beford_d_task
dim_oride_passenger_base_prev_day_task >> app_oride_user_funnel_beford_d_task