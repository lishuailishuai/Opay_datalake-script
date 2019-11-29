# -*- coding: utf-8 -*-
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

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 11, 27),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('ods_opay_binlog_user_order_hi',
                  schedule_interval="15 * * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一小时分区
binlog_user_order_prev_hour_task = HivePartitionSensor(
    task_id="binlog_user_order_prev_hour_task",
    table="binlog_user_order",
    partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
    schema="opay_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opay_dw_ods"
table_name = "ods_opay_binlog_user_order_hi"
hdfs_path = "s3a://opay-bi/opay_dw_ods/binlog/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, execution_date, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opay_dw_ods", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "dt={pt}/hour={now_hour}".format(pt=ds, now_hour=execution_date.strftime("%H")),
         "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def ods_binlog_user_order_hi_sql_task(ds, hour):
    HQL = '''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        INSERT OVERWRITE TABLE {db}.{table} partition(dt,hour)
        SELECT  get_json_object(after,'$.id')              AS id
               ,get_json_object(after,'$.order_no')        AS order_no
               ,get_json_object(after,'$.order_user_type') AS order_user_type
               ,get_json_object(after,'$.user_id')         AS user_id
               ,get_json_object(after,'$.amount')          AS amount
               ,get_json_object(after,'$.country')         AS country
               ,get_json_object(after,'$.currency')        AS currency
               ,get_json_object(after,'$.flow_type')       AS flow_type
               ,get_json_object(after,'$.fee_amount')      AS fee_amount
               ,get_json_object(after,'$.fee_pattern')     AS fee_pattern
               ,get_json_object(after,'$.order_status')    AS order_status
               ,get_json_object(after,'$.merchant_id')     AS merchant_id
               ,get_json_object(after,'$.service_type')    AS service_type
               ,get_json_object(after,'$.pay_channel')     AS pay_channel
               ,get_json_object(after,'$.business_type')   AS business_type
               ,get_json_object(after,'$.other_trader')    AS other_trader
               ,get_json_object(after,'$.after_balance')   AS after_balance
               ,get_json_object(after,'$.create_time')     AS create_time
               ,get_json_object(after,'$.update_time')     AS update_time
               ,bussine_id
               ,op
               ,ts_ms
               ,dt
               ,hour
        FROM opay_source.binlog_user_order
        WHERE dt = '{pt}' and hour = '{now_hour}'
        ;

        ;
'''.format(
        pt=ds,
        now_day='{{macros.ds_add(ds, +1)}}',
        now_hour=hour,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    # 读取sql
    _sql = ods_binlog_user_order_hi_sql_task(ds, v_hour)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """

    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true", v_hour)


ods_binlog_user_order_hi_task = PythonOperator(
    task_id='ods_binlog_user_order_hi_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

binlog_user_order_prev_hour_task >> ods_binlog_user_order_hi_task
