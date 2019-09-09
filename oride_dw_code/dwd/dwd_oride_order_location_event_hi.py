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

dag = airflow.DAG('dwd_oride_order_location_event_hi',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一小时分区
dependence_dwd_oride_location_driver_event_hi_prev_hour_task = HivePartitionSensor(
    task_id="dwd_oride_driver_location_event_hi_prev_hour_task",
    table="dwd_oride_driver_location_event_hi",
    partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一小时分区
dependence_dwd_oride_passanger_location_event_hi_prev_hour_task = HivePartitionSensor(
    task_id="dwd_oride_passanger_location_event_hi_prev_hour_task",
    table="dwd_oride_passanger_location_event_hi",
    partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
    schema="oride_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_order_location_event_hi"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_location_driver_event_hi_task = HiveOperator(
    task_id='dwd_oride_location_driver_event_hi_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt,hour)
        
        select 
        d.order_id , --订单id
        p.user_id , --用户id
        d.driver_id , --司机id
        d.accept_order_click_lat , --司机accept_order_click，事件，纬度
        d.accept_order_click_lng , --司机accept_order_click，事件，经度
        d.confirm_arrive_click_arrived_lat , --司机confirm_arrive_click_arrived，事件，纬度
        d.confirm_arrive_click_arrived_lng  , --司机confirm_arrive_click_arrived，事件，经度
        d.pick_up_passengers_sliding_arrived_lat , --司机pick_up_passengers_sliding_arrived，事件，纬度
        d.pick_up_passengers_sliding_arrived_lng , --司机pick_up_passengers_sliding_arrived，事件，经度
        d.start_ride_sliding_lat , --司机start_ride_sliding，事件，纬度
        d.start_ride_sliding_lng , --司机start_ride_sliding，事件，经度
        d.start_ride_sliding_arrived_lat , --司机start_ride_sliding_arrived，事件，纬度
        d.start_ride_sliding_arrived_lng , --司机start_ride_sliding_arrived，事件，经度
        p.looking_for_a_driver_show_lat , --乘客looking_for_a_driver_show，事件，纬度
        p.looking_for_a_driver_show_lng ,--乘客looking_for_a_driver_show，事件，经度
        p.successful_order_show_lat ,--乘客successful_order_show，事件，纬度
        p.successful_order_show_lng ,--乘客successful_order_show，事件，经度
        p.start_ride_show_lat ,--乘客start_ride_show，事件，纬度
        p.start_ride_show_lng ,--乘客start_ride_show，事件，经度
        p.complete_the_order_show_lat,--乘客complete_the_order_show，事件，纬度
        p.complete_the_order_show_lng, --乘客complete_the_order_show，事件，经度
        p.rider_arrive_show_lat ,  --乘客rider_arrive_show，事件，纬度
        p.rider_arrive_show_lng , --乘客rider_arrive_show，事件，经度
        
        'nal' as country_code,
        '{now_day}' as dt,
        '{now_hour}' as hour
        
        from 
        
        (
            select 
            order_id , 
            driver_id ,
            accept_order_click_lat ,
            accept_order_click_lng ,
            confirm_arrive_click_arrived_lat ,
            confirm_arrive_click_arrived_lng  ,
            pick_up_passengers_sliding_arrived_lat ,
            pick_up_passengers_sliding_arrived_lng ,
            start_ride_sliding_lat ,
            start_ride_sliding_lng ,
            start_ride_sliding_arrived_lat ,
            start_ride_sliding_arrived_lng 
            from 
            oride_dw.dwd_oride_driver_location_event_hi
            where dt = '{now_day}' and hour = '{now_hour}'
        ) d 
        left join 
        (	
            select 
            
            order_id , 
            user_id ,
            looking_for_a_driver_show_lat ,
            looking_for_a_driver_show_lng ,
            successful_order_show_lat ,
            successful_order_show_lng ,
            start_ride_show_lat ,
            start_ride_show_lat ,
            start_ride_show_lng ,
            complete_the_order_show_lat,
            complete_the_order_show_lng，
            rider_arrive_show_lat ,
            rider_arrive_show_lng ,
            from 
            oride_dw.dwd_oride_passanger_location_event_hi
            where dt = '{now_day}' and hour = '{now_hour}'
        ) p on d.order_id = p.order_id

        ;


'''.format(
        pt='{{ds}}',
        now_day='{{ds}}',
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name
    ),
    dag=dag
)


def check_key_data(ds, **kargs):
    # 主键重复校验
    HQL_DQC = '''
    SELECT count(1) as nm
    FROM
     (SELECT order_id,
             user_id,
             driver_id,
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      and hour = '{now_hour}'
      GROUP BY 
      order_id,
      user_id,
      driver_id 
      HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=ds,
        now_hour='{{ execution_date.strftime("%H") }}',
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
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}/hour={{ execution_date.strftime("%H") }}'
    ),
    dag=dag)

dependence_dwd_oride_location_driver_event_hi_prev_hour_task >> sleep_time
dependence_dwd_oride_passanger_location_event_hi_prev_hour_task >> sleep_time
sleep_time >> dwd_oride_location_driver_event_hi_task >> task_check_key_data >> touchz_data_success
