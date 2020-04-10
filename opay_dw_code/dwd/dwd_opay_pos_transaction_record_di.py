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


dag = airflow.DAG('dwd_opay_pos_transaction_record_di',
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

ods_sqoop_base_user_pos_transaction_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_pos_transaction_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/user_pos_transaction_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_merchant_pos_transaction_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_merchant_pos_transaction_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/merchant_pos_transaction_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_opay_terminal_base_df_prev_day_task = OssSensor(
   task_id='dim_opay_terminal_base_df_prev_day_task',
   bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_terminal_base_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_opay_bd_agent_df_prev_day_task = OssSensor(
    task_id='dim_opay_bd_agent_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_bd_agent_df/country_code=NG",
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
table_name = "dwd_opay_pos_transaction_record_di"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name
pos_provider_share_0922_fee=eval(Variable.get("pos_provider_share_0922_fee"))
msc_cost_0922_fee=eval(Variable.get("msc_cost_0922_fee"))
config = eval(Variable.get("opay_time_zone_config"))

def dwd_opay_pos_transaction_record_di_sql_task(ds):
    HQL='''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with dim_user_merchant_data as (
            select 
                trader_id, trader_name, trader_role, trader_kyc_level, if(state is null or state = '', '-', state) as state
            from (
                select 
                    user_id as trader_id, concat(first_name, ' ', middle_name, ' ', surname) as trader_name, `role` as trader_role, kyc_level as trader_kyc_level, state,
                    row_number() over(partition by user_id order by update_time desc) rn
                from opay_dw_ods.ods_sqoop_base_user_di
                where dt <= '{pt}'
            ) uf where rn = 1
            union all
            select 
                merchant_id as trader_id, merchant_name as trader_name, merchant_type as trader_role, '-' as trader_kyc_level, '-' as state
            from opay_dw_ods.ods_sqoop_base_merchant_df
            where dt = if('{pt}' <= '2019-12-11', '2019-12-11', '{pt}')
        ),
        terminal_data as (
            select 
                pos_id, terminal_id 
            from opay_dw.dim_opay_terminal_base_df 
            where dt = if('{pt}' <= '2020-01-01', '2020-01-01', '{pt}')
        ),
        bd_agent_data as (
            select 
                user_id, bd_admin_user_id, agent_status as bd_agent_status
            from opay_dw.dim_opay_bd_agent_df
            where dt = '{pt}'
        )
    insert overwrite table {db}.{table} 
    partition(country_code, dt)
    
    select 
        t1.order_no, t1.amount, t1.stamp_duty, t1.currency, t1.originator_type, t2.trader_role as originator_role, t2.trader_kyc_level as originator_kyc_level, t1.originator_id, t2.trader_name as originator_name,
        t1.affiliate_terminal_id, t1.affiliate_terminal_provider_id, t1.affiliate_bank_code, 
        t1.pos_trade_req_id, t1.transaction_reference, t1.retrieval_reference_number,
        t1.create_time, t1.update_time, t1.country, t1.order_status, t1.error_code, t1.error_msg, t1.transaction_type, t1.accounting_status, 
        'pos' as top_consume_scenario, 'pos' as sub_consume_scenario,
        t1.fee_amount, t1.fee_pattern, t1.outward_id, t1.outward_type, 
        nvl(t3.pos_id, '-') as pos_id, t2.state, 
        case
            when order_status = 'SUCCESS' then if(cast(t1.amount * {pos_provider_share_0922_fee}  as decimal(10,2)) > 100000, 100000, cast(t1.amount * {pos_provider_share_0922_fee}  as decimal(10,2)))
            else 0
        end as provider_share_amount,
        
        case
            when order_status = 'SUCCESS' then if(cast(t1.amount * {msc_cost_0922_fee}  as decimal(10,2)) > 100000, 100000, cast(t1.amount * {msc_cost_0922_fee}  as decimal(10,2)))
            else 0
        end as msc_cost_amount,
        t4.bd_admin_user_id, t4.bd_agent_status,
        'NG' as country_code,
        '{pt}' dt
        
    from (
        select 
            order_no, amount, stamp_duty, currency, 'USER' as originator_type, user_id as originator_id, 
            terminal_id as affiliate_terminal_id, terminal_provider_id as affiliate_terminal_provider_id, bank_code as affiliate_bank_code,
            pos_trade_req_id, transaction_reference, retrieval_reference_number,
            default.localTime("{config}", 'NG',create_time, 0) as create_time,
            default.localTime("{config}", 'NG',update_time, 0) as update_time,  
            country, order_status, channel_code as error_code, channel_msg as error_msg, transaction_type, accounting_status,
            nvl(fee_amount, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, '-' as outward_id, '-' as outward_type
        from opay_dw_ods.ods_sqoop_base_user_pos_transaction_record_di
        where dt = '{pt}'
        union all
        select 
            order_no, amount, stamp_duty, currency, 'MERCHANT' as originator_type, merchant_id as originator_id, 
            
            terminal_id as affiliate_terminal_id, terminal_provider_id as affiliate_terminal_provider_id, bank_code as affiliate_bank_code,
            pos_trade_req_id, transaction_reference, retrieval_reference_number,
            default.localTime("{config}", 'NG',create_time, 0) as create_time,
            default.localTime("{config}", 'NG',update_time, 0) as update_time,  
            country, order_status, channel_code as error_code, channel_msg as error_msg, transaction_type, accounting_status,
            nvl(fee_amount, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, '-' as outward_id, '-' as outward_type
        from opay_dw_ods.ods_sqoop_base_merchant_pos_transaction_record_di
        where dt = '{pt}'
    ) t1 
    left join dim_user_merchant_data t2 on t1.originator_id = t2.trader_id
    left join terminal_data t3 on t1.affiliate_terminal_id = t3.terminal_id
    left join bd_agent_data t4 on t1.originator_id = t4.user_id
    
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        pos_provider_share_0922_fee=pos_provider_share_0922_fee,
        msc_cost_0922_fee=msc_cost_0922_fee,
        config=config
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_pos_transaction_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_opay_pos_transaction_record_di_task = PythonOperator(
    task_id='dwd_opay_pos_transaction_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task >> dwd_opay_pos_transaction_record_di_task
ods_sqoop_base_merchant_df_prev_day_task >> dwd_opay_pos_transaction_record_di_task
ods_sqoop_base_user_pos_transaction_record_di_prev_day_task >> dwd_opay_pos_transaction_record_di_task
dim_opay_terminal_base_df_prev_day_task >> dwd_opay_pos_transaction_record_di_task
ods_sqoop_base_merchant_pos_transaction_record_di_prev_day_task >> dwd_opay_pos_transaction_record_di_task
dim_opay_bd_agent_df_prev_day_task >> dwd_opay_pos_transaction_record_di_task