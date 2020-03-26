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
from airflow.sensors import OssSensor

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_user_last_visit_df',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##
dwm_opay_user_last_visit_df_prev_day_task = OssSensor(
    task_id='dwm_opay_user_last_visit_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_user_last_visit_df/country_code=NG",
        pt='{{macros.ds_add(ds, -1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"

table_name = "dwm_opay_user_last_visit_df"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def dwm_opay_user_last_visit_df_sql_task(ds):
    HQL = '''


    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    
    with 
    user_last_visit_di as (
        select 
           uid as user_id, 
           from_unixtime(cast(max(cast(t as bigint)) / 1000 as bigint)+3600) as last_visit_time,
           'NG' as country_code
        from opay_source.service_data_tracking_app 
        where concat(dt, hour) 
            between date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23')
                and date_format(date_add('{pt}', 1), 'yyyy-MM-dd 00')
            and t is not null and t != ''
            and servicedatatrackingapp='api_graphql_web_id' 
            and ev in ('login version one','login version two','login version two again','registerSuccess to login')
            and date_format(from_unixtime(cast(cast(t as bigint) / 1000 as bigint)+3600) , 'yyyy-MM-dd')= '{pt}'
        group by uid
    
    )
    
    insert overwrite table {db}.{table} partition(country_code, dt)
    select 
        coalesce(t1.user_id, t0.user_id) as user_id,
        coalesce(t1.last_visit_time, t0.last_visit_time) as last_visit_time,
        coalesce(t1.country_code, t0.country_code) as country_code,
        '{pt}' as dt
    from (
        SELECT 
            user_id, last_visit_time,
            country_code 
        from opay_dw.dwm_opay_user_last_visit_df where dt = date_sub('{pt}', 1)
    ) t0 full join (
        select 
            user_id, last_visit_time,
            country_code
        from user_last_visit_di
    ) t1 on t0.user_id = t1.user_id;


    '''.format(
        pt=ds,
        yesterday=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_opay_user_last_visit_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_opay_user_last_visit_df_task = PythonOperator(
    task_id='dwm_opay_user_last_visit_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwm_opay_user_last_visit_df_prev_day_task >> dwm_opay_user_last_visit_df_task


