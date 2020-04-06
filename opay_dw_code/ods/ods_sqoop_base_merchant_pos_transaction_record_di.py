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
    'start_date': datetime(2020, 4, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('ods_sqoop_base_merchant_pos_transaction_record_di',
                  schedule_interval="30 00 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

ods_binlog_base_merchant_pos_transaction_record_check_hi_task = OssSensor(
    task_id='ods_binlog_base_merchant_pos_transaction_record_check_hi_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=22/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_transaction_db.opay_transaction.merchant_pos_transaction_record",
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
        {"dag": dag, "db": "opay_dw_ods", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw_ods"

table_name = "ods_sqoop_base_merchant_pos_transaction_record_di"
hdfs_path = "oss://opay-datalake/opay_dw_sqoop_di/opay_transaction/merchant_pos_transaction_record"
config = eval(Variable.get("opay_time_zone_config"))


def ods_sqoop_base_merchant_pos_transaction_record_di_sql_task(ds):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table {db}.{table} partition (dt)
    SELECT 
        id,
        order_no,
        terminal_id,
        pos_trade_req_id,
        transaction_reference,
        retrieval_reference_number,
        merchant_id,
        currency,
        amount,
        fee_amount,
        fee_pattern,
        channel_code,
        channel_msg,
        payment_date,
        transaction_type,
        order_status,
        accounting_status,
        terminal_provider_id,
        bank_code,
        country,
        from_unixtime(cast(cast(create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') create_time,
        from_unixtime(cast(cast(update_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') update_time,
        stamp_duty,
        '{pt}'
    from 
        (select *,row_number() over(partition by id order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
         FROM opay_dw_ods.ods_binlog_base_merchant_pos_transaction_record_hi
         where concat(dt,' ',hour) between '{pt_y} 23' and '{pt} 22' and `__deleted` = 'false'
         ) m 
    where rn=1

    '''.format(
        pt=ds,
        pt_y=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name,
        config=config
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = ods_sqoop_base_merchant_pos_transaction_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


ods_sqoop_base_merchant_pos_transaction_record_di_task = PythonOperator(
    task_id='ods_sqoop_base_merchant_pos_transaction_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_binlog_base_merchant_pos_transaction_record_check_hi_task >> ods_sqoop_base_merchant_pos_transaction_record_di_task

