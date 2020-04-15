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
    'start_date': datetime(2020, 2, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_pos_cube_d',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dim_opay_terminal_base_df_prev_day_task = OssSensor(
   task_id='dim_opay_terminal_base_df_prev_day_task',
  bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_terminal_base_df/country_code=NG",
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
ods_sqoop_base_user_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_user/user",
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

table_name = "app_opay_pos_cube_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_pos_cube_d_sql_task(ds):
    HQL = '''

    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with 
        um_data as (
            SELECT 
                user_id,ROLE,state
            FROM (
                SELECT user_id,ROLE,state,row_number()over(partition BY user_id ORDER BY update_time DESC) rn
                FROM opay_dw_ods.ods_sqoop_base_user_di where dt<='{pt}' and role='agent') m
            WHERE rn=1

        )
    
    INSERT overwrite TABLE {db}.{table} partition (country_code, dt)
    
    select 
        nvl(pos_id, 'ALL') as pos_id,
        nvl(region, 'ALL') as region,
        nvl(state, 'ALL') as state,
        nvl(order_status, 'ALL') as order_status,
        'trans' as target_type,
        count(distinct affiliate_terminal_id) active_terms,
        count(distinct originator_id) active_agents,
        0 as bind_agents,
        0 as bind_terms,
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
            where dt = '{pt}' 
                  and date_format(create_time, 'yyyy-MM-dd') = '{pt}'
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
    union all
    select
        nvl(pos_id, 'ALL') as pos_id,
        nvl(region, 'ALL') as region,
        nvl(state, 'ALL') as state,
        'ALL' as order_status,
        'terminal' as target_type,
        0 as active_terms,
        0 as active_agents,
        count(distinct user_id) as bind_agents,
        count(distinct terminal_id) as bind_terms,
        country_code,
        '{pt}'
    from (
        select
            pos_id, terminal_id, user_id, t4.state, nvl(t5.region, '-') as region, country_code
        from (
            SELECT 
                pos_id, b.state, terminal_id, a.owner_id user_id, country_code
            FROM (select * from opay_dw.dim_opay_terminal_base_df
                  WHERE dt='{pt}' AND bind_status='Y') a
            inner join um_data b 
            on a.owner_id=b.user_id
        ) t4 left join (
            select
                state, region
            from opay_dw.dim_opay_region_state_mapping_df
            where dt = if('{pt}' <= '2020-02-10', '2020-02-10', '{pt}')
        ) t5 on t5.state = t4.state
    ) t6
    group by pos_id, state, region, country_code
    GROUPING SETS (
        (pos_id, country_code),
        (pos_id, region, country_code),
        (pos_id, state, country_code),
        (state, country_code),
        (region, country_code),
        (country_code)
    )
    
    
    
    

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
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_pos_cube_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_opay_pos_cube_d_task = PythonOperator(
    task_id='app_opay_pos_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dim_opay_terminal_base_df_prev_day_task >> app_opay_pos_cube_d_task
dwd_opay_pos_transaction_record_di_task >> app_opay_pos_cube_d_task
ods_sqoop_base_user_di_prev_day_task >> app_opay_pos_cube_d_task


