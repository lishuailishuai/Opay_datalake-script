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
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os
from plugins.CountriesAppFrame import CountriesAppFrame

args = {
    'owner': 'xiedong',
    'start_date': datetime(2019, 12, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = airflow.DAG('dwd_opay_transaction_record_di',
                  schedule_interval="30 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##
#依赖前一天分区
dwd_opay_life_payment_record_di_task = OssSensor(
    task_id='dwd_opay_life_payment_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_life_payment_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_transfer_of_account_record_di_task = OssSensor(
    task_id='dwd_opay_transfer_of_account_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transfer_of_account_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_cash_to_card_record_di_task = OssSensor(
    task_id='dwd_opay_cash_to_card_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cash_to_card_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_topup_with_card_record_di_task = OssSensor(
    task_id='dwd_opay_topup_with_card_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_topup_with_card_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_receive_money_record_di_task = OssSensor(
    task_id='dwd_opay_receive_money_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_receive_money_record_di/country_code=NG",
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

dwd_opay_cico_record_di_task = OssSensor(
    task_id='dwd_opay_cico_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cico_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
# dwd_opay_easycash_record_di_task = OssSensor(
#     task_id='dwd_opay_easycash_record_di_task',
#     bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
#         hdfs_path_str="opay/opay_dw/dwd_opay_easycash_record_di/country_code=NG",
#         pt='{{ds}}'
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
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
table_name = "dwd_opay_transaction_record_di"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name


def dwd_opay_transaction_record_di_sql_task(ds):
    HQL='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    set mapred.max.split.size=1000000;
    
    with 
        dwd_opay_account_recharge_di as (
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_bank_card_no_encrypted as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'TopupWithCard' as sub_service_type, 
                order_status, error_code, error_msg, client_source, pay_way, top_consume_scenario, sub_consume_scenario, 
                fee_amount, fee_pattern, outward_id, outward_type, state,
                country_code, dt
            from opay_dw.dwd_opay_topup_with_card_record_di
            where dt = '{pt}'
            union all
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_bank_account_code as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'receivemoney' as sub_service_type, 
                order_status, error_code, error_msg, '-' as client_source, '-' as pay_way, top_consume_scenario, sub_consume_scenario, 
                fee_amount, fee_pattern, outward_id, outward_type, state,
                country_code, dt
            from opay_dw.dwd_opay_receive_money_record_di
            where dt = '{pt}'
            union all
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_terminal_id as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'pos' as sub_service_type, 
                order_status, error_code, error_msg, '-' as client_source, '-' as pay_way, top_consume_scenario, sub_consume_scenario, 
                fee_amount, fee_pattern, outward_id, outward_type, state,
                country_code, dt
            from opay_dw.dwd_opay_pos_transaction_record_di
            where dt = '{pt}'
            union all
            select 
                order_no, amount, currency, 
                originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'IN' as originator_money_flow,
                '-' as affiliate_type, '-' as affiliate_role, affiliate_mobile as affiliate_id, '-' as affiliate_name, 'OUT' as affiliate_money_flow,
                create_time, update_time, 'Account Recharge' as top_service_type, 'easycash' as sub_service_type, 
                order_status, error_code, error_msg, '-' as client_source, '-' as pay_way, top_consume_scenario, sub_consume_scenario, 
                fee_amount, fee_pattern, outward_id, outward_type, state,
                country_code, dt
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
        order_status, error_code, error_msg, client_source, pay_way, top_consume_scenario, sub_consume_scenario,
        fee_amount, fee_pattern, outward_id, outward_type, state,
        country_code, dt
    from opay_dw.dwd_opay_life_payment_record_di 
    where dt = '{pt}' 
    union all
    select 
        order_no, amount, currency, 
        originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'OUT' as originator_money_flow,
        affiliate_type, affiliate_role, affiliate_id, affiliate_name, 'IN' as affiliate_money_flow,
        create_time, update_time, top_service_type, sub_service_type, 
        order_status, error_code, error_msg, client_source, pay_way, top_consume_scenario, sub_consume_scenario, 
        fee_amount, fee_pattern, outward_id, outward_type, state,
        country_code, dt
    from opay_dw.dwd_opay_transfer_of_account_record_di 
    where dt = '{pt}' 
    union all
    select 
        order_no, amount, currency, 
        originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'OUT' as originator_money_flow,
        affiliate_type, affiliate_role, affiliate_id, affiliate_name, 'IN' as affiliate_money_flow,
        create_time, update_time, top_service_type, sub_service_type, 
        order_status, error_code, error_msg, client_source, pay_way, top_consume_scenario, sub_consume_scenario, 
        fee_amount, fee_pattern, outward_id, outward_type, state,
        country_code, dt
    from opay_dw.dwd_opay_cico_record_di 
    where dt = '{pt}' 
    union all
    select 
        order_no, amount, currency, 
        originator_type, originator_role, originator_kyc_level, originator_id, originator_name, 'OUT' as originator_money_flow,
        '-' as affiliate_type, '-' as affiliate_role, affiliate_bank_account_no_encrypted as affiliate_id, '-' as affiliate_name, 'IN' as affiliate_money_flow,
        create_time, update_time, top_service_type, sub_service_type, 
        order_status, error_code, error_msg, client_source, pay_way, top_consume_scenario, sub_consume_scenario, 
        fee_amount, fee_pattern, outward_id, outward_type, state,
        country_code, dt
    from opay_dw.dwd_opay_cash_to_card_record_di 
    where dt = '{pt}'
    union all
    select * from dwd_opay_account_recharge_di
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
            "is_country_partition": "false",
            "is_result_force_exist": "false",
            "execute_time": v_execution_time,
            "is_hour_task": "false",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_transaction_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_opay_transaction_record_di_task = PythonOperator(
    task_id='dwd_opay_transaction_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwd_opay_life_payment_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_transfer_of_account_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_cash_to_card_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_topup_with_card_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_receive_money_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_pos_transaction_record_di_task >> dwd_opay_transaction_record_di_task
dwd_opay_cico_record_di_task >> dwd_opay_transaction_record_di_task