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

dag = airflow.DAG('dwd_opay_user_upgrade_agent_df',
                  schedule_interval="10 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##
dim_opay_user_base_df_prev_day_task = OssSensor(
   task_id='dim_opay_user_base_df_prev_day_task',
  bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_user_base_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
 )

ods_sqoop_base_user_upgrade_df_check_task = OssSensor(
    task_id='ods_sqoop_base_user_upgrade_df_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_user/user_upgrade",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_user_reseller_df_check_task = OssSensor(
    task_id='ods_sqoop_base_user_reseller_df_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_user/user_reseller",
        pt='{{ds}}'
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

table_name = "dwd_opay_user_upgrade_agent_df"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))


def dwd_opay_user_upgrade_agent_df_sql_task(ds):
    HQL = '''
    set  hive.exec.max.dynamic.partitions.pernode=1000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with user_upgrade_df as (
        select 
            user_id, accept_overload_user_id, confirmed_overload_user_id, 
            default.localTime("{config}", 'NG',upgrade_date, 0) as upgrade_time
        from (
            select
                user_id, 
                accept_overload_user_Id as accept_overload_user_id, confirmed_overload_user_Id as confirmed_overload_user_id, 
                upgrade_date,
                row_number() over(partition by user_id order by upgrade_date desc) rn
            from opay_dw_ods.ods_sqoop_base_user_upgrade_df where dt = '{pt}'
        ) t0 where rn = 1
    )
    insert overwrite table {db}.{table} partition(country_code, dt)
    select 
        t0.user_id, t0.role, 
        nvl(t1.upgrade_time, t0.create_time) as upgrade_time,
        accept_overload_user_id,
        confirmed_overload_user_id,
        if(t2.aggregator_id is null, 'N', 'Y') as is_reseller,
        t2.update_time as reseller_time,
        t2.aggregator_id,
        'NG' as country_code,
        '{pt}' as dt
    from (
        SELECT
            user_id, role, create_time
        from opay_dw.dim_opay_user_base_df where dt = '{pt}' and role = 'agent'
    ) t0 
    left join user_upgrade_df t1 on t0.user_id = t1.user_id
    left join (
        select
            user_id, merchant_id as aggregator_id, 
            default.localTime("{config}", 'NG',update_time, 0) as update_time
        from opay_dw_ods.ods_sqoop_base_user_reseller_df where dt = '{pt}' and nvl(physical_del, 'false') != 'true'
    ) t2 on t0.user_id = t2.user_id

    '''.format(
        pt=ds,
        yesterday=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name,
        config = config
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_user_upgrade_agent_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_opay_user_upgrade_agent_df_task = PythonOperator(
    task_id='dwd_opay_user_upgrade_agent_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dim_opay_user_base_df_prev_day_task >> dwd_opay_user_upgrade_agent_df_task
ods_sqoop_base_user_reseller_df_check_task >> dwd_opay_user_upgrade_agent_df_task
ods_sqoop_base_user_upgrade_df_check_task >> dwd_opay_user_upgrade_agent_df_task


