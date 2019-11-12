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

dag = airflow.DAG('dm_oride_driver_base_d',
                  schedule_interval="50 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwm_oride_driver_base_df_prev_day_task = UFileSensor(
    task_id='dwm_oride_driver_base_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dm_oride_driver_base_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

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

def dm_oride_driver_base_d_sql_task(ds):

    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    
    select product_id,
       city_id,
       sum(driver_finished_dur) as driver_finished_dur,
       --司机支付完单做单时长
       sum(driver_cannel_pick_dur) as cannel_pick_dur,
       --司机订单取消接驾时长，包含各种取消方数据
       sum(driver_free_dur) as driver_free_dur,
       --司机空闲时长
       sum(driver_request_order_cnt) as driver_request_order_cnt,  --之前叫succ_push_order_cnt
       --司机接单量  
       sum(driver_finish_online_dur) as finish_driver_online_dur,
       --完单司机在线时长
       sum(driver_click_order_cnt) as driver_click_order_cnt,
       --司机点击应答订单量(accept_click节点)
       sum(driver_show_order_cnt) as driver_pushed_order_cnt,  
       --司机被推送订单量(accept_show节点)  字段名称修改
       sum(driver_billing_dur) as driver_billing_dur,
       --司机计费时长
       sum(strong_driver_finish_online_dur) as strong_finish_driver_online_dur,
       --强派单完单司机在线时长
       country_code,
       --国家编码
       dt
       
from oride_dw.dwm_oride_driver_base_df
where dt='{pt}'
group by product_id,
       city_id,
       country_code,
       dt

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
    _sql = dm_oride_driver_base_d_sql_task(ds)

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


dm_oride_driver_base_d_task = PythonOperator(
    task_id='dm_oride_driver_base_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwm_oride_driver_base_df_prev_day_task  >> dm_oride_driver_base_d_task