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
    'start_date': datetime(2020, 2, 3),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_pos_cube_w',
                  schedule_interval="00 03 * * 1",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dim_opay_pos_terminal_base_df_prev_day_task = OssSensor(
   task_id='dim_opay_pos_terminal_base_df_prev_day_task',
   bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_pos_terminal_base_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_pos_transaction_record_di_task = OssSensor(
    task_id='dwd_opay_pos_transaction_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_pos_transaction_record_di/country_code=NG",
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
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=airflow.macros.ds_add(ds, +6)), "timeout": "800"}
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

table_name = "app_opay_pos_cube_w"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_pos_cube_w_sql_task(ds):
    HQL = '''

    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    
    INSERT overwrite TABLE {db}.{table} partition (country_code, dt)
    
    select 
        nvl(pos_id, 'ALL') as pos_id,
        nvl(region, 'ALL') as region,
        nvl(state, 'ALL') as state,
        nvl(order_status, 'ALL') as order_status,
        'trans' as target_type,
        count(distinct affiliate_terminal_id) active_terms,
        count(distinct originator_id) active_agents,
        country_code,
        '{pt}'
    from (
        select 
            pos_id, nvl(region, '-') as region, t1.state, order_status, affiliate_terminal_id, originator_id,
            country_code
        from (
            select 
                pos_id, state, order_status, country_code, affiliate_terminal_id, originator_id
            from opay_dw.dwd_opay_pos_transaction_record_di
            where dt between date_sub(next_day('{pt}', 'mo'), 7)  and date_sub(next_day('{pt}', 'mo'), 1) 
                and create_time BETWEEN date_format(date_sub(next_day('{pt}', 'mo'), 8), 'yyyy-MM-dd 23') AND date_format(date_sub(next_day('{pt}', 'mo'), 1), 'yyyy-MM-dd 23')
                and originator_type = 'USER' 
        ) t1 left join (
            select
                state, region
            from opay_dw.dim_opay_region_state_mapping_df
            where dt = if('{pt}' <= '2020-02-10', '2020-02-10', '{pt}')
        ) t2 on t1.state = t2.state
    ) t3
    group by pos_id, region, state, order_status, country_code
    GROUPING SETS ( 
        (pos_id, country_code),
        (pos_id, order_status, country_code),
        (pos_id, region, order_status, country_code),
        (pos_id, state, order_status, country_code),
        (region, order_status, country_code),
        (region, country_code),
        (state, order_status, country_code),
        (state, country_code),
        (order_status, country_code),
        (country_code)
    )
    
    

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_pos_cube_w_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success('{pt}'.format(pt=airflow.macros.ds_add(ds, +6)), db_name, table_name, hdfs_path, "true", "true")


app_opay_pos_cube_w_task = PythonOperator(
    task_id='app_opay_pos_cube_w_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opay_pos_terminal_base_df_prev_day_task >> app_opay_pos_cube_w_task
dwd_opay_pos_transaction_record_di_task >> app_opay_pos_cube_w_task


