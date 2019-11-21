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

dag = airflow.DAG('dwd_oride_order_location_event_di',
                  schedule_interval="30 4 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwd_oride_driver_location_event_hi_prev_day_task = UFileSensor(
    task_id='dependence_dwd_oride_driver_location_event_hi_prev_day_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_location_event_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwd_oride_passanger_location_event_hi_prev_day_task = UFileSensor(
    task_id='dependence_dwd_oride_passanger_location_event_hi_prev_day_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_passanger_location_event_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_order_location_event_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##


def dwd_oride_order_location_event_di_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT OVERWRITE TABLE {db}.{table} partition(country_code,dt)
    SELECT  d.order_id --订单id 
           ,p.user_id --用户id 
           ,d.driver_id --司机id 
           ,d.accept_order_click_lat --司机accept_order_click，事件，纬度 
           ,d.accept_order_click_lng --司机accept_order_click，事件，经度 
           ,d.confirm_arrive_click_arrived_lat --司机confirm_arrive_click_arrived，事件，纬度 
           ,d.confirm_arrive_click_arrived_lng --司机confirm_arrive_click_arrived，事件，经度 
           ,d.pick_up_passengers_sliding_arrived_lat --司机pick_up_passengers_sliding_arrived，事件，纬度 
           ,d.pick_up_passengers_sliding_arrived_lng --司机pick_up_passengers_sliding_arrived，事件，经度 
           ,d.start_ride_sliding_lat --司机start_ride_sliding，事件，纬度 
           ,d.start_ride_sliding_lng --司机start_ride_sliding，事件，经度 
           ,d.start_ride_sliding_arrived_lat --司机start_ride_sliding_arrived，事件，纬度 
           ,d.start_ride_sliding_arrived_lng --司机start_ride_sliding_arrived，事件，经度 
           ,p.looking_for_a_driver_show_lat --乘客looking_for_a_driver_show，事件，纬度 
           ,p.looking_for_a_driver_show_lng --乘客looking_for_a_driver_show，事件，经度 
           ,p.successful_order_show_lat --乘客successful_order_show，事件，纬度 
           ,p.successful_order_show_lng --乘客successful_order_show，事件，经度 
           ,p.start_ride_show_lat --乘客start_ride_show，事件，纬度 
           ,p.start_ride_show_lng --乘客start_ride_show，事件，经度 
           ,p.complete_the_order_show_lat --乘客complete_the_order_show，事件，纬度 
           ,p.complete_the_order_show_lng --乘客complete_the_order_show，事件，经度 
           ,p.rider_arrive_show_lat --乘客rider_arrive_show，事件，纬度 
           ,p.rider_arrive_show_lng --乘客rider_arrive_show，事件，经度 
           ,'nal'  AS country_code 
           ,'{pt}' AS dt
    FROM 
    (
        SELECT  order_id 
               ,driver_id 
               ,accept_order_click_lat 
               ,accept_order_click_lng 
               ,confirm_arrive_click_arrived_lat 
               ,confirm_arrive_click_arrived_lng 
               ,pick_up_passengers_sliding_arrived_lat 
               ,pick_up_passengers_sliding_arrived_lng 
               ,start_ride_sliding_lat 
               ,start_ride_sliding_lng 
               ,start_ride_sliding_arrived_lat 
               ,start_ride_sliding_arrived_lng
        FROM oride_dw.dwd_oride_driver_location_event_di
        WHERE dt = '{pt}'  
    ) d
    LEFT JOIN 
    (
        SELECT  order_id 
               ,user_id 
               ,looking_for_a_driver_show_lat 
               ,looking_for_a_driver_show_lng 
               ,successful_order_show_lat 
               ,successful_order_show_lng 
               ,start_ride_show_lat 
               ,start_ride_show_lng 
               ,complete_the_order_show_lat 
               ,complete_the_order_show_lng 
               ,rider_arrive_show_lat 
               ,rider_arrive_show_lng
        FROM oride_dw.dwd_oride_passanger_location_event_di
        WHERE dt = '{pt}'  
    ) p
    ON d.order_id = p.order_id 
    ;

'''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct (concat(order_id,'_',nvl(user_id,0),'_',nvl(driver_id,0))))  as cnt
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
    _sql = dwd_oride_order_location_event_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_order_location_event_di_task = PythonOperator(
    task_id='dwd_oride_order_location_event_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_driver_location_event_hi_prev_day_task >> \
dependence_dwd_oride_passanger_location_event_hi_prev_day_task >> \
sleep_time >> \
dwd_oride_order_location_event_di_task
