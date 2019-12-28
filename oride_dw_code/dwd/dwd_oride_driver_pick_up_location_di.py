# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
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
from airflow.sensors import OssSensor

args = {
        'owner': 'lijialong',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwd_oride_driver_pick_up_location_di', 
    schedule_interval="30 01 * * *",
    default_args=args,
    catchup=False) 

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name="dwd_oride_driver_pick_up_location_di"

##----------------------------------------- 依赖 ---------------------------------------## 
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    #依赖前一天分区
    oride_client_event_detail_prev_day_task = UFileSensor(
        task_id="oride_client_event_detail_prev_day_task",
        filepath='{hdfs_path_str}/country_code=nal/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:
    print("成功")
    oride_client_event_detail_prev_day_task = OssSensor(
        task_id="oride_client_event_detail_prev_day_task",
        bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)



task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 

def dwd_oride_driver_pick_up_location_di_sql_task(ds):
    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT driver_id,
       --主键（driver_id,order_id,pick_up_status）

       city_id,
       --定位城市

       product_id,
       --接单司机类型，1=专车，2=快车

       current_point,
       --用户当前位置

       order_id,
       --订单号

       pick_up_status,
       --接驾类型(1 接用户-已接到乘客,2 接用户-到达终点)

       'nal' AS country_code,
       --国家码字段

       '{pt}' AS dt
FROM
  (SELECT user_id AS driver_id,
          get_json_object(event_value, '$.current_point') AS current_point,
          get_json_object(event_value, '$.order_id') AS order_id,
          get_json_object(event_value, '$.city_id') AS city_id,
          get_json_object(event_value, '$.serv_type') AS product_id,
          1 AS pick_up_status
   FROM oride_dw.dwd_oride_client_event_detail_hi
   WHERE dt='{pt}'
     AND event_name='start_ride_sliding'
   UNION ALL SELECT user_id AS driver_id,
                    get_json_object(event_value, '$.current_point') AS current_point,
                    get_json_object(event_value, '$.order_id') AS order_id,
                    get_json_object(event_value, '$.city_id') AS city_id,
                    get_json_object(event_value, '$.serv_type') AS product_id,
                    2 AS pick_up_status
   FROM oride_dw.dwd_oride_client_event_detail_hi
   WHERE dt='{pt}'
     AND event_name='start_ride_sliding_arrived') t1
GROUP BY driver_id,
         city_id,
         product_id,
         current_point,
         order_id,
         pick_up_status;

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
    _sql = dwd_oride_driver_pick_up_location_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    #check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_driver_pick_up_location_di_task = PythonOperator(
    task_id='dwd_oride_driver_pick_up_location_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

oride_client_event_detail_prev_day_task>>dwd_oride_driver_pick_up_location_di_task


