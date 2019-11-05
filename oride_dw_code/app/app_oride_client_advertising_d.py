# -*- coding: utf-8 -*-
"""
oride 客户端广告数据
"""
import airflow
from datetime import datetime, timedelta
from utils.validate_metrics_utils import *
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.hive_operator import HiveOperator
import time
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 11, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_client_advertising_d',
    schedule_interval="00 03 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)

# 依赖 ufile://opay-datalake/oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal/dt=2019-08-09/hour=06'
dependence_dwd_oride_client_event_detail_hi = UFileSensor(
    task_id="dependence_dwd_oride_client_event_detail_hi",
    filepath='{hdfs_path_str}/dt={pt}/hour=23'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
        pt='{{ ds }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag
)

# create_table = HiveOperator(
#    task_id='create_table_task',
#    hql="""
#        CREATE TABLE IF NOT EXISTS app_oride_client_advertising_d (
#            event_name string comment '广告事件名称',
#            pv int comment '广告PV量',
#            uv int comment '广告UV量'
#        )
#        partitioned by (
#            dt string comment '数据产出日期'
#        )
#        STORED AS ORC
#        LOCATION 'ufile://opay-datalake/oride/oride_dw/app_oride_client_advertising_d'
#    """,
#    schema="oride_dw",
#    dag=dag
# )


import_result = HiveOperator(
    task_id='import_result_task',
    hql="""
        SET mapred.job.queue.name=root.users.airflow;
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        INSERT OVERWRITE TABLE app_oride_client_advertising_d PARTITION (dt)
        SELECT  
            CASE 
                WHEN event_name='homepage_window_show' THEN '首页弹窗广告展示'
                WHEN event_name='homepage_window_click' THEN '首页弹窗广告点击' 
                WHEN event_name='homepage_window_close_click' THEN '首页弹窗广告点击关闭' 
                WHEN event_name='banner_window_show' THEN 'banner广告展示' 
                WHEN event_name='banner_window_click' THEN 'banner广告点击'
                WHEN event_name='banner_window_close_click' THEN 'banner广告点击关闭' 
	            WHEN event_name='top_activities_click' THEN '点击ORide首页活动中心' 
	            WHEN event_name='top_activities_show' THEN '活动中心展示' 
	            WHEN event_name='activities_picture_click' THEN '在活动中心点击广告图' 
	            WHEN event_name='finished_window_show' THEN '支付完成页弹窗展示' 
	            WHEN event_name='finished_window_click' THEN '支付完成页弹窗点击' 
	            WHEN event_name='finishede_window_close_click' THEN '支付完成页弹窗点击关闭' 
	            ELSE '其他' END, 
            count(1) AS pv, 
            count(distinct ip) AS uv, 
            dt 
        FROM dwd_oride_client_event_detail_hi 
        WHERE dt = '{{ ds }}' AND 
            event_name IN (
                'homepage_window_show',
                'homepage_window_click',
                'homepage_window_close_click',
                'banner_window_show',
                'banner_window_click',
                'banner_window_close_click',
                'top_activities_click',
                'top_activities_show',
                'activities_picture_click',
                'finished_window_show',
                'finished_window_click',
                'finishede_window_close_click'
            )
        GROUP BY dt, event_name
    """,
    schema="oride_dw",
    dag=dag
)

dependence_dwd_oride_client_event_detail_hi >> sleep_time >> import_result
