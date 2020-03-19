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
    'owner': 'liushuzhen',
    'start_date': datetime(2020, 3, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_user_upgrade_agent_df',
                  schedule_interval="30 02 * * *",
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

table_name = "dwm_opay_user_upgrade_agent_df"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))


def dwm_opay_user_upgrade_agent_df_sql_task(ds):
    HQL = '''
    set  hive.exec.max.dynamic.partitions.pernode=1000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    select 
        t0.user_id, t0.role, 
        nvl(t1.upgrade_time, t0.create_time) as upgrade_time,
        accept_overload_user_id,
        confirmed_overload_user_id,
        if(t2.aggregator_id is null, 'N', 'Y') as is_reseller,
        t2.update_time as reseller_time,
        t2.aggregator_id
    from (
        SELECT
            user_id, role, create_time
        from dim_opay_user_base_df where dt = '{pt}' and role = 'agent'
    ) t0 left join (
        select
            user_id, 
            accept_overload_user_Id as accept_overload_user_id, confirmed_overload_user_Id as confirmed_overload_user_id, 
            default.localTime("{config}", 'NG',upgrade_date, 0) as upgrade_time
        from opay_dw_ods.ods_sqoop_base_user_upgrade_df where dt = '{pt}'
    ) t1 on t0.user_id = t1.accept_overload_user_Id
    left join (
        select
            user_id, merchant_id as aggregator_id, 
            default.localTime("{config}", 'NG',update_time, 0) as update_time
        from opay_dw_ods.ods_sqoop_base_user_reseller_df where dt = '{pt}' 
    ) t2 on t0.user_id = t2.user_id

    '''.format(
        pt=ds,
        yesterday=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name,
        config = config
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_opay_user_upgrade_agent_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_opay_user_upgrade_agent_df_task = PythonOperator(
    task_id='dwm_opay_user_upgrade_agent_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opay_user_base_df_prev_day_task >> dwm_opay_user_upgrade_agent_df_task
ods_sqoop_base_user_reseller_df_check_task >> dwm_opay_user_upgrade_agent_df_task
ods_sqoop_base_user_upgrade_df_check_task >> dwm_opay_user_upgrade_agent_df_task


