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
    'start_date': datetime(2019, 11, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = airflow.DAG('dwd_opay_transaction_record_di',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##
#依赖前一天分区
dwd_opay_life_payment_record_di_task = UFileSensor(
    task_id='dwd_opay_life_payment_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_life_payment_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_transfer_of_account_record_di_task = UFileSensor(
    task_id='dwd_opay_transfer_of_account_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transfer_of_account_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_cash_to_card_record_di_task = UFileSensor(
    task_id='dwd_opay_cash_to_card_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cash_to_card_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_topup_with_card_record_di_task = UFileSensor(
    task_id='dwd_opay_topup_with_card_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_topup_with_card_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_receive_money_record_di_task = UFileSensor(
    task_id='dwd_opay_receive_money_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_receive_money_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_pos_transaction_record_di_task = UFileSensor(
    task_id='dwd_opay_pos_transaction_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_pos_transaction_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dwd_opay_easycash_record_di_task = UFileSensor(
    task_id='dwd_opay_easycash_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_easycash_record_di/country_code=NG",
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
        {"db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
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
table_name = "dwd_opay_transaction_record_di"
hdfs_path="ufile://opay-datalake/opay/opay_dw/" + table_name


def dwd_opay_transaction_record_di_sql_task(ds):
    HQL='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with 
        dwd_opay_account_recharge_di as (
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_bank_card_no_encrypted as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'TopupWithCard' as sub_service_type, 
                order_status, error_code, error_msg, client_source, pay_way, country_code, dt
            from opay_dw.dwd_opay_topup_with_card_record_di
            where dt = '{pt}'
            union all
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_bank_account_code as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'receivemoney' as sub_service_type, 
                order_status, error_code, error_msg, '-' as client_source, '-' as pay_way, country_code, dt
            from opay_dw.dwd_opay_receive_money_record_di
            where dt = '{pt}'
            union all
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_terminal_id as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'pos' as sub_service_type, 
                order_status, error_code, error_msg, '-' as client_source, '-' as pay_way, country_code, dt
            from opay_dw.dwd_opay_pos_transaction_record_di
            where dt = '{pt}'
            union all
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_mobile as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'pos' as sub_service_type, 
                order_status, error_code, error_msg, '-' as client_source, '-' as pay_way, country_code, dt
            from opay_dw.dwd_opay_easycash_record_di
            where dt = '{pt}'
        )
    insert overwrite table {db}.{table} 
    partition(country_code, dt)
    select 
        order_no, amount, currency, 
        originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'OUT' as originator_money_flow,
        affiliate_type, affiliate_role, affiliate_id, affiliate_name, 'IN' as affiliate_money_flow,
        create_time, update_time, top_service_type, sub_service_type, 
        order_status, error_code, error_msg, client_source, pay_way, country_code, dt
    from opay_dw.dwd_opay_life_payment_record_di 
    where dt = '{pt}' 
    union
    select 
        order_no, amount, currency, 
        originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'OUT' as originator_money_flow,
        affiliate_type, affiliate_role, affiliate_id, affiliate_name, 'IN' as affiliate_money_flow,
        create_time, update_time, top_service_type, sub_service_type, 
        order_status, error_code, error_msg, client_source, pay_way, country_code, dt
    from opay_dw.dwd_opay_transfer_of_account_record_di 
    where dt = '{pt}' 
    union
    select 
        order_no, amount, currency, 
        originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'OUT' as originator_money_flow,
        '-' as affiliate_type, '-' as affiliate_role, affiliate_bank_account_no_encrypted as affiliate_id, '-' as affiliate_name, 'IN' as affiliate_money_flow,
        create_time, update_time, top_service_type, sub_service_type, 
        order_status, error_code, error_msg, client_source, pay_way, country_code, dt
    from opay_dw.dwd_opay_cash_to_card_record_di 
    where dt = '{pt}'
    union
    select * from dwd_opay_account_recharge_di
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL



# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opay_transaction_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_opay_transaction_record_di_task = PythonOperator(
    task_id='dwd_opay_transaction_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_life_payment_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_transfer_of_account_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_cash_to_card_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_topup_with_card_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_receive_money_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_pos_transaction_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_easycash_record_di_task >> dwd_opay_transaction_record_di_task