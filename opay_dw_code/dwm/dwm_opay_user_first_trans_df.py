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
from plugins.CountriesAppFrame import CountriesAppFrame

args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_user_first_trans_df',
                  schedule_interval="40 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opay_user_transaction_record_di_prev_day_task = OssSensor(
    task_id='dwd_opay_user_transaction_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_user_transaction_record_di/country_code=NG",
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
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
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

table_name = "dwm_opay_user_first_trans_df"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def dwm_opay_user_first_trans_df_sql_task(ds):
    HQL = '''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with user_first_trans_di as (
        select 
            order_no, trans_time, amount, currency,
            user_id, user_role, 
            top_service_type, sub_service_type, top_consume_scenario, sub_consume_scenario, originator_or_affiliate,
            country_code
        from (
            select 
                order_no, create_time as trans_time, amount, currency,
                user_id, user_role, 
                top_service_type, sub_service_type, top_consume_scenario, sub_consume_scenario, originator_or_affiliate,
                country_code, 
                row_number() over(partition by user_id order by create_time) rn
            from opay_dw.dwd_opay_user_transaction_record_di
            where dt = '{pt}' and user_id is not null and user_id != ''
        ) t0 where rn = 1
    )
    insert overwrite table {db}.{table} partition(country_code, dt)
    select 
        order_no, trans_time, amount, currency, 
        top_service_type, sub_service_type, 
        user_id, user_role, originator_or_affiliate,
        top_consume_scenario, sub_consume_scenario,
        country_code,
        '{pt}'
    from opay_dw.dwm_opay_user_first_trans_df where dt = date_sub('{pt}', 1)
    union all
    select 
        order_no, trans_time, amount, currency, 
        top_service_type, sub_service_type, 
        t0.user_id, user_role, originator_or_affiliate,
        top_consume_scenario, sub_consume_scenario, country_code, 
        '{pt}'
    from (
        select 
            order_no, trans_time, amount, currency,
            user_id, user_role, 
            top_service_type, sub_service_type, top_consume_scenario, sub_consume_scenario, originator_or_affiliate,
            country_code
        from user_first_trans_di
    ) t0 left join (
        select 
            user_id
        from opay_dw.dwm_opay_user_first_trans_df where dt = date_sub('{pt}', 1)
    ) t1 on t0.user_id = t1.user_id
    where t1.user_id is null
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_execution_time = kwargs.get('v_execution_time')
    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_execution_time,
            "is_hour_task": "false",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_opay_user_first_trans_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwm_opay_user_first_trans_df_task = PythonOperator(
    task_id='dwm_opay_user_first_trans_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwd_opay_user_transaction_record_di_prev_day_task >> dwm_opay_user_first_trans_df_task

