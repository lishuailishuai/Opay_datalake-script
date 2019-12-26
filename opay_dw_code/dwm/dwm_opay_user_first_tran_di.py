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
    'start_date': datetime(2019, 12, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_user_first_tran_di',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

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

table_name = "dwm_opay_user_first_tran_di"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def dwm_opay_user_first_tran_di_sql_task(ds):
    HQL = '''
    SET mapreduce.job.queuename= opay_collects;
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    INSERT overwrite TABLE opay_dw.dwm_opay_user_first_tran_di (country_code,dt)
SELECT a.order_no,
       a.create_time,
       a.amount,
       a.top_service_type,
       a.sub_service_type,
       a.originator_id,
       a.originator_type,
       a.originator_role,
       a.originator_kyc_level,
       a.originator_name,
       a.top_consume_scenario,
       a.sub_consume_scenario,
       a.client_source,
       a.country_code,
       a.dt
FROM
  (SELECT m1.*
   FROM
     (SELECT m.*,
             row_number()over(partition BY originator_id
                              ORDER BY create_time) rn
      FROM opay_dw.dwd_opay_transaction_record_di m
      WHERE order_status='SUCCESS'
        AND dt='{pt}') m1
   WHERE rn=1) a
LEFT JOIN
  (SELECT *
   FROM opay_dw.dwm_opay_user_first_tran_di
   WHERE dt<'{pt}') b ON a.originator_id=b.originator_id
WHERE b.originator_id IS NULL


    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_opay_user_first_tran_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_opay_user_first_tran_di_task = PythonOperator(
    task_id='dwm_opay_user_first_tran_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_transaction_record_di_prev_day_task >> dwm_opay_user_first_tran_di_task

