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
    'start_date': datetime(2019, 11, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_driver_track_data_di',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_ods_log_driver_track_data_hi_task = HivePartitionSensor(
    task_id="dependence_ods_log_driver_track_data_hi_task",
    table="ods_log_driver_track_data_hi",
    partition="dt='{{ ds }}'",
    schema="oride_dw_ods",
    poke_interval=60,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_driver_track_data_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##
def dwd_driver_track_data_di_sql_task(ds):
    HQL ='''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
   
    INSERT OVERWRITE TABLE oride_dw.dwd_driver_track_data_di partition(country_code,dt)
    SELECT  id          AS driver_id --司机ID 
           ,order_id    AS order_id --订单ID 
           ,lng --经度 
           ,lat --纬度 
           ,direction --方向 
           ,mode --模式 
           ,city_id --城市ID 
           ,`timestamp` AS log_timestamp --日志时间 
           ,serv_mode 
           ,serv_status 
           ,gps_time --卫星真实时间 
           ,speed --定位速度 
           ,accuracy --定位精度 
           ,provider --位置提供者 
           ,trip_id 
           ,order_ids 
           ,mock 
           ,'nal'       AS country_code 
           ,dt
    FROM oride_dw_ods.ods_log_driver_track_data_hi
    WHERE dt='{pt}';  
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_driver_track_data_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_driver_track_data_di_task = PythonOperator(
    task_id='dwd_driver_track_data_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_ods_log_driver_track_data_hi_task>>dwd_driver_track_data_di_task

