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
    'start_date': datetime(2020, 1, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_pos_report_d',
                  schedule_interval="00 03 * * *",
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

dwd_opay_transaction_record_di_prev_day_task = OssSensor(
    task_id='dwd_opay_transaction_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transaction_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"

table_name = "app_opay_pos_report_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_pos_report_d_sql_task(ds):
    HQL = '''

    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    WITH pos AS
      (SELECT *
       FROM opay_dw.dim_opay_pos_terminal_base_df
       WHERE dt='{pt}'
         AND bind_status='Y'
         AND create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')),
         tran AS
      (SELECT *
       FROM opay_dw.dwd_opay_transaction_record_di
       WHERE dt='{pt}'
         AND create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')
         AND sub_service_type='pos'
         AND order_status='SUCCESS' )
    INSERT overwrite TABLE {db}.{TABLE} partition (dt='{pt}')
    SELECT active_terms,
           bind_terms,
           bind_agents
    FROM
      (SELECT '{pt}' dt,
                     count(DISTINCT affiliate_id) active_terms
       FROM tran) a
    LEFT JOIN
      (SELECT '{pt}' dt,
                     count(CASE
                               WHEN originator_role='agent' THEN terminal_id
                           END) bind_terms
       FROM pos) b ON a.dt=b.dt
    LEFT JOIN
      (SELECT '{pt}' dt,
                     count(DISTINCT user_id) bind_agents
       FROM pos
       where originator_role='agent') c ON a.dt=c.dt

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_pos_report_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_pos_report_d_task = PythonOperator(
    task_id='app_opay_pos_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opay_pos_terminal_base_df_prev_day_task >> app_opay_pos_report_d_task
dwd_opay_transaction_record_di_prev_day_task >> app_opay_pos_report_d_task


