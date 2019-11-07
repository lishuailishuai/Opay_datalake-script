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

##
# 央行月报汇报指标
#
args = {
    'owner': 'xiedong',
    'start_date': datetime(2019, 11, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_opay_service_provider_df',
                 schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##------declare variables end ------##


##----------------------------------------- 依赖 ---------------------------------------##
# ods_service_provider_base_df_task = UFileSensor(
#     task_id='ods_service_provider_base_df_task',
#     filepath='{hdfs_path_str}/service_provider_init.data'.format(
#         hdfs_path_str="opay/opay_dw/ods_service_provider_base_df",
#         pt='{{ds}}'
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
# )


##---- hive operator ---##
dim_opay_service_provider_df_task = HiveOperator(
    task_id='dim_opay_service_provider_df_task',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;

    set hive.exec.parallel=true;

    insert overwrite table opay_dw.dim_opay_service_provider_df
    select 
        id, name, service_type, provider_type 
    from opay_dw_ods.ods_service_provider_base_df
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw',
    dag=dag
)
##---- hive operator end ---##


ods_service_provider_base_df_task>>dim_opay_service_provider_df_task