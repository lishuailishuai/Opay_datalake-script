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
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 9, 23),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_mark_df',
                  schedule_interval="40 1 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dwd_oride_order_base_include_test_df_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dim_oride_city_prev_day_task = HivePartitionSensor(
    task_id="dim_oride_city_prev_day_task",
    table="dim_oride_city",
    partition="dt='{{ds}}'",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_weather_per_10min_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_weather_per_10min_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/bi/weather_per_10min",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_user_comment_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_data_user_comment_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_user_comment",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1500"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_order_mark_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##
def dwd_oride_order_mark_df_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT t.order_id,  --订单ID
           t.create_time_s as create_time, --下单时间
           passenger_id,  --乘客ID
           status,--订单状态 (0: wait assign, 1: pick up passenger, 2: wait passenger, 3: send passenger, 4: arrive destination, 5: finished, 6: cancel)
           2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 as start_distance,--相邻两单起点距离
           2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 as end_distance, --相邻两单终点距离
        IF(status IN (4,5), 1,
            IF(t.order_id=order_id2, 1,
                IF(ABS(t.create_time2-t.create_time)<=1800 AND
                    2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 <= 1000 AND
                    2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 <= 1000, 0, 1
                )
            )
        ) AS is_valid,  --订单有效标志:1有效 0无效
        if(weather.city is not null and weather.run_time_hour is not null and weather.mins is not null,1,0) as is_wet_order, --是否湿单
        t.driver_id, --司机ID
        com.score, -- 该订单的评分
        'nal' AS country_code,
        '{pt}' AS dt
    FROM (
        SELECT
            order_id,
            driver_id,
            passenger_id,
            city_id,
            start_lng,
            start_lat,
            end_lng,
            end_lat,
            create_time as create_time,
            from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss') as create_time_s,
            
            from_unixtime(create_time,'yyyy-MM-dd HH') as create_time_hour,  --订单所在小时，为了获取天气状况
            floor(cast(minute(from_unixtime(create_time)) as int) / 10)*10 as create_time_mins, --订单所在十分钟采集时间，为了获取天气状况git
            create_date,
            status,
            LEAD(create_time,1,create_time) OVER(PARTITION BY passenger_id ORDER BY create_time) create_time2,
            LEAD(start_lng,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) start_lng2,
            LEAD(start_lat,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) start_lat2,
            LEAD(end_lng,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) end_lng2,
            LEAD(end_lat,1,0) OVER(PARTITION BY passenger_id ORDER BY create_time) end_lat2,
            LEAD(order_id,1,order_id) OVER(PARTITION BY passenger_id ORDER BY create_time) order_id2
        FROM oride_dw.dwd_oride_order_base_include_test_di
        WHERE dt='{pt}'
            AND city_id<>'999001' --去除测试数据
             and driver_id<>1
        ) t
        left join 
        (select * from oride_dw.dim_oride_city where dt='{pt}') cit
        on t.city_id=cit.city_id
        left join
        (SELECT city,
                from_unixtime(unix_timestamp(run_time),'yyyy-MM-dd HH') run_time_hour,
                minute(from_unixtime(unix_timestamp(run_time))) mins
         FROM oride_dw_ods.ods_sqoop_base_weather_per_10min_df
         WHERE dt = '{pt}'
          AND weather IN ('Thundershower',
                          'Light rain',
                          'Rain',
                          'Thunderstorm',
                          'A shower')
          AND daliy = '{pt}') weather
          on lower(cit.city_name)=lower(weather.city)
          and weather.run_time_hour=t.create_time_hour
          and weather.mins=t.create_time_mins
          left join
          (select * from oride_dw_ods.ods_sqoop_base_data_user_comment_df
          where dt='{pt}') com
          on t.order_id=com.order_id
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct order_id) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
      and country_code in ('nal')
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_oride_order_mark_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_order_mark_df_task = PythonOperator(
    task_id='dwd_oride_order_mark_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_order_base_include_test_df_prev_day_task >> \
dim_oride_city_prev_day_task >> \
ods_sqoop_base_weather_per_10min_df_prev_day_task >> \
ods_sqoop_base_data_user_comment_df_prev_day_task >> \
dwd_oride_order_mark_df_task
