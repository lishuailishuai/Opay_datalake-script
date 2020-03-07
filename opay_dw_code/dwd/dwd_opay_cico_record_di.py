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
from airflow.sensors import OssSensor

##
# 央行月报汇报指标
#
args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 6),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opay_cico_record_di',
                  schedule_interval="20 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##
ods_sqoop_base_user_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_user/user",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_merchant_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_merchant_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_merchant/merchant",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_cash_in_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_cash_in_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/cash_in_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_cash_out_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_cash_out_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/cash_out_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_bd_agent_df_prev_day_task = OssSensor(
    task_id='ods_bd_agent_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_agent_crm/bd_agent",
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
        {"dag":dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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
table_name = "dwd_opay_cico_record_di"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def dwd_opay_cico_record_di_sql_task(ds, ds_nodash):
    HQL = '''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    create table if not exists test_db.cico_user_temp_{pt_str} as 
        select 
                trader_id, trader_name, trader_role, trader_kyc_level, if(state is null or state = '', '-', state) as state
            from (
                select 
                    user_id as trader_id, concat(first_name, ' ', middle_name, ' ', surname) as trader_name, `role` as trader_role, kyc_level as trader_kyc_level, state,
                    row_number() over(partition by user_id order by update_time desc) rn
                from opay_dw_ods.ods_sqoop_base_user_di
                where dt <= '{pt}'
            ) uf where rn = 1;
    with 
        bd_agent_data as (
            select 
                cast(opay_id as string) as opay_id, bd_id, agent_status as bd_agent_status
            from opay_dw_ods.ods_sqoop_base_bd_agent_df
            where dt = '{pt}'
        ),
        ci_data as (
            select 
                order_no, amount, currency, originator_type, originator_id, affiliate_type, affiliate_id, payment_order_no, 
                    create_time, update_time, country, sub_service_type, order_status,
                    error_code, error_msg, client_source, pay_way, business_type, top_consume_scenario, sub_consume_scenario,
                    fee_amount, fee_pattern, outward_id, outward_type, 
                    bd_id as bd_admin_user_id, bd_agent_status
            from (
                select 
                    order_no, amount, currency, 'USER' as originator_type, sender_id as originator_id, 'USER' as affiliate_type, recipient_id as affiliate_id, '-' as payment_order_no, 
                    create_time, update_time, country, 'Cash In' as sub_service_type, order_status,
                    error_code, error_msg, client_source, pay_channel as pay_way, '-' as business_type, 'Cash In' as top_consume_scenario, 'Cash In' as sub_consume_scenario,
                    nvl(fee_amount, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, nvl(outward_id, '-') as outward_id, nvl(outward_type, '-') as outward_type
                from opay_dw_ods.ods_sqoop_base_cash_in_record_di
                where dt = '{pt}' 
            ) ci left join bd_agent_data ba on ci.originator_id = ba.opay_id
        ),
        co_data as (
            select
                order_no, amount, currency, originator_type, originator_id, affiliate_type, affiliate_id, payment_order_no, 
                    create_time, update_time, country, sub_service_type, order_status,
                    error_code, error_msg, client_source, pay_way, business_type, top_consume_scenario, sub_consume_scenario,
                    fee_amount, fee_pattern, outward_id, outward_type,
                    bd_id as bd_admin_user_id, bd_agent_status
            from (
                select 
                    order_no, amount, currency, 'USER' as originator_type, sender_id as originator_id, 'USER' as affiliate_type, recipient_id as affiliate_id, '-' as payment_order_no, 
                    create_time, update_time, country, 'Cash Out' as sub_service_type, order_status,
                    error_code, error_msg, client_source, pay_channel as pay_way, '-' as business_type, 'Cash Out' as top_consume_scenario, 'Cash Out' as sub_consume_scenario,
                    nvl(fee_amount, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, nvl(outward_id, '-') as outward_id, nvl(outward_type, '-') as outward_type
                from opay_dw_ods.ods_sqoop_base_cash_out_record_di
                where dt = '{pt}' 
            ) co left join bd_agent_data ba on co.affiliate_id = ba.opay_id
        )
    insert overwrite table {db}.{table} 
    partition(country_code, dt)

    select 
        t1.order_no, t1.amount, t1.currency,
        t1.originator_type, t2.trader_role as originator_role, t2.trader_kyc_level as originator_kyc_level, t1.originator_id, t2.trader_name as originator_name,
        t1.affiliate_type, t3.trader_role as affiliate_role, t1.affiliate_id, t3.trader_name as affiliate_name, 
        case 
            when t1.originator_type = 'MERCHANT' and t1.affiliate_type = 'MERCHANT' then 'm2m'
            when t1.originator_type = 'MERCHANT' and t1.affiliate_type = 'USER' and t3.trader_role = 'customer' then 'm2c'
            when t1.originator_type = 'MERCHANT' and t1.affiliate_type = 'USER' and t3.trader_role = 'agent' then 'm2a'
            when t2.trader_role = 'customer' and t1.affiliate_type = 'MERCHANT' then 'c2m'
            when t2.trader_role = 'agent' and t1.affiliate_type = 'MERCHANT' then 'a2m'
            when t2.trader_role = 'agent' and t3.trader_role = 'customer' then 'a2c'
            when t2.trader_role = 'agent' and t3.trader_role = 'agent' then 'a2a'
            when t2.trader_role = 'customer' and t3.trader_role = 'agent' then 'c2a'
            when t2.trader_role = 'customer' and t3.trader_role = 'customer' then 'c2c'
            else 'unknow'
            end as payment_relation_id,
        t1.payment_order_no, t1.create_time, t1.update_time, t1.country, 'Transfer of Account' as top_service_type, t1.sub_service_type, t1.order_status,
        t1.error_code, t1.error_msg, t1.client_source, t1.pay_way, t1.business_type, 
        t1.top_consume_scenario, t1.sub_consume_scenario,
        t1.fee_amount, t1.fee_pattern, t1.outward_id, t1.outward_type,
        bd_admin_user_id, bd_agent_status, t2.state,
        'NG' as country_code,
        '{pt}' dt
    from (
        select * from ci_data
        union all
        select * from co_data
    ) t1 
    left join test_db.cico_user_temp_{pt_str} t2 on t1.originator_id = t2.trader_id
    left join test_db.cico_user_temp_{pt_str} t3 on t1.affiliate_id = t3.trader_id;
    DROP TABLE IF EXISTS test_db.cico_user_temp_{pt_str}
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        pt_str=ds_nodash
    )
    return HQL


# 主流程
def execution_data_task_id(ds, ds_nodash, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opay_cico_record_di_sql_task(ds, ds_nodash)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_opay_cico_record_di_task = PythonOperator(
    task_id='dwd_opay_cico_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task >> dwd_opay_cico_record_di_task
ods_sqoop_base_merchant_df_prev_day_task >> dwd_opay_cico_record_di_task
ods_sqoop_base_cash_in_record_di_prev_day_task >> dwd_opay_cico_record_di_task
ods_sqoop_base_cash_out_record_di_prev_day_task >> dwd_opay_cico_record_di_task
ods_bd_agent_df_prev_day_task >> dwd_opay_cico_record_di_task
