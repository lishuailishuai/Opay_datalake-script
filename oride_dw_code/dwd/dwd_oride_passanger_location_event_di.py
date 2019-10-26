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

dag = airflow.DAG('dwd_oride_passanger_location_event_di',
                  schedule_interval="30 4 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一小时分区
dwd_oride_passanger_location_event_hi_prev_day_task = UFileSensor(
    task_id='dwd_oride_passanger_location_event_hi_prev_day_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_passanger_location_event_hi",
        pt='{{ds}}',
        hour='23'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_passanger_location_event_di"
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

dwd_oride_passanger_location_event_di_task = HiveOperator(
    task_id='dwd_oride_passanger_location_event_di_task',
    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt)
        
        select 
        order_id , 
        user_id  ,
        replace(concat_ws(',',collect_set(looking_for_a_driver_show_lat)),',','')  ,
        replace(concat_ws(',',collect_set(looking_for_a_driver_show_lng)),',','') ,
        replace(concat_ws(',',collect_set(successful_order_show_lat)),',','') ,
        replace(concat_ws(',',collect_set(successful_order_show_lng)),',','')  ,
        replace(concat_ws(',',collect_set(start_ride_show_lat)),',','') ,
        replace(concat_ws(',',collect_set(start_ride_show_lng)),',','') ,
        replace(concat_ws(',',collect_set(complete_the_order_show_lat)),',','') ,
        replace(concat_ws(',',collect_set(complete_the_order_show_lng)),',','') ,
        replace(concat_ws(',',collect_set(rider_arrive_show_lat)),',','') ,
        replace(concat_ws(',',collect_set(rider_arrive_show_lng)),',',''),
        
        
        'nal' as country_code,
        '{pt}' as dt
        
        
        from 
        oride_dw.dwd_oride_passanger_location_event_hi
        where dt = '{pt}'
        group by order_id ,
        user_id 

        ;


'''.format(
        pt='{{ds}}',
        now_day='{{ds}}',
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
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      GROUP BY 
      order_id,user_id 
      HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=ds,
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

dwd_oride_passanger_location_event_hi_prev_day_task >> sleep_time >> dwd_oride_passanger_location_event_di_task >> task_check_key_data >> touchz_data_success
