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
    'owner': 'lishuai',
    'start_date': datetime(2020, 1, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_user_device_d',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dwd_opay_client_event_base_di_prev_day_task = HivePartitionSensor(
    task_id="dwd_opay_client_event_base_di_prev_day_task",
    table="dwd_opay_client_event_base_di",
    partition="dt='{{ ds }}'",
    schema="opay_dw",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

app_opay_user_device_d_prev_day_task = OssSensor(
    task_id='app_opay_user_device_d_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/app_opay_user_device_d/country_code=nal",
        pt='{{macros.ds_add(ds, -1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "app_opay_user_device_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


##----------------------------------------- 脚本 ---------------------------------------##

def app_opay_user_device_d_sql_task(ds):
    HQL = '''

    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table opay_dw.app_opay_user_device_d partition(country_code,dt)
    select 
    if(a.user_id is null,b.user_id,a.user_id),
    if(a.device_id is null,b.device_id,a.device_id),
    if(a.mobile is null,b.mobile,a.mobile),
    if(b.device_id is not null,b.bb,a.`timestamp`),
    'nal' as country_code,
    '{pt}' as dt
    from(
    select * from 
    opay_dw.app_opay_user_device_d 
    where dt=date_sub('{pt}',1)
    )a
    full join
    (
    select user_id,
        device_id,
         mobile,
             bb 
    from(
        select
            user_id ,
            device_id ,
            mobile,
            server_timestamp bb,
            row_number() over(partition by user_id,device_id order by server_timestamp desc) ff
        from opay_dw.dwd_opay_client_event_base_di
        where dt = '{pt}' and device_id!=''
       -- group by common.user_id,common.device_id
    ) c
    where c.ff=1

    )b
    on a.device_id=b.device_id and a.user_id=b.user_id;

'''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_user_device_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opay_user_device_d_task = PythonOperator(
    task_id='app_opay_user_device_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_client_event_base_di_prev_day_task >> app_opay_user_device_d_task
app_opay_user_device_d_prev_day_task >> app_opay_user_device_d_task