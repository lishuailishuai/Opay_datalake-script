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

dag = airflow.DAG('app_opay_channel_transaction_sum_d',
                  schedule_interval="00 03 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opay_channel_transaction_base_di_prev_day_task = OssSensor(
    task_id='dwd_opay_channel_transaction_base_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_channel_transaction_base_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
#ods_sqoop_base_card_bin_df_prev_day_task = OssSensor(
#   task_id='ods_sqoop_base_card_bin_df_prev_day_task',
#    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
#        hdfs_path_str="opay_dw_sqoop/opay_channel/card_bin",
#        pt='{{ds}}'
#    ),
#    bucket_name='opay-datalake',
#    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#    dag=dag
# )

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

table_name = "app_opay_channel_transaction_sum_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_channel_transaction_sum_d_sql_task(ds):
    HQL = '''
    
    
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table {db}.{table} partition (country_code,dt)
     
        SELECT pay_channel,
               out_channel_id,
               supply_item,
               response_code,
               transaction_status,
               nvl(b.flag,0) flag,
               user_type,
               sum(amount) AS tran_amt,
               count(1) AS tran_c,
               c.brand,
               c.bank_name,
               a.country_code,
               a.dt
        FROM
          (SELECT pay_channel,out_channel_id,supply_item,response_code,transaction_status,user_type,dt,bank_response_message,
                  amount,country_code,
                  cast(substr(AES_DECRYPT(UNHEX(bank_card_no), UNHEX('4132E08EA055A2B852DE8C214C885C2A')),1,6) as STRING) bank
           FROM opay_dw.dwd_opay_channel_transaction_base_di
           WHERE dt='{pt}'
                and date_format(create_time, 'yyyy-MM-dd') = dt ) a
        LEFT JOIN opay_dw.dim_opay_bank_response_message_df b ON a.bank_response_message=b.bank_response_message
        LEFT JOIN (select * from opay_dw_ods.ods_sqoop_base_card_bin_df where dt='2020-02-11') c on a.bank=c.bin
        GROUP BY pay_channel,
                 out_channel_id,
                 supply_item,
                 response_code,
                 transaction_status,
                 nvl(b.flag,0),
                 user_type,
                 c.brand,
                 c.bank_name,
                 a.dt,
                 a.country_code

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
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_channel_transaction_sum_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_opay_channel_transaction_sum_d_task = PythonOperator(
    task_id='app_opay_channel_transaction_sum_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

# ods_sqoop_base_card_bin_df_prev_day_task >> app_opay_channel_transaction_sum_d_task

dwd_opay_channel_transaction_base_di_prev_day_task >> app_opay_channel_transaction_sum_d_task


