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


dag = airflow.DAG('dwd_opay_topup_with_card_record_di',
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

ods_sqoop_base_user_topup_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_topup_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/user_topup_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_merchant_topup_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_merchant_topup_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/merchant_topup_record",
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
table_name = "dwd_opay_topup_with_card_record_di"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))


def dwd_opay_topup_with_card_record_di_sql_task(ds):
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
        )
    insert overwrite table {db}.{table} 
    partition(country_code, dt)
    
    select 
        t1.order_no, t1.amount, t1.currency, t1.originator_type, t2.trader_role as originator_role, t2.trader_kyc_level as originator_kyc_level, t1.originator_id, t2.trader_name as originator_name,
        t1.affiliate_bind_card_id, t1.affiliate_bank_code, t1.affiliate_bank_name, t1.affiliate_bank_account_no_encrypted, t1.affiliate_bank_card_no_encrypted, t1.affiliate_bank_mobile, t1.affiliate_bank_email,
        t1.affiliate_bank_scheme, t1.payment_order_no, t1.create_time, t1.update_time, t1.country, t1.order_status, t1.error_code, t1.error_msg, t1.client_source,
        t1.pay_way, t1.next_step, t1.out_channel_id, t1.business_type, t1.accounting_status, 
        'TopupWithCard' as top_consume_scenario, 'TopupWithCard' as sub_consume_scenario, 
        t1.fee_amount, t1.fee_pattern, t1.outward_id, t1.outward_type, t2.state,
         case
            when order_status = 'SUCCESS' and out_channel_id in('FLUTTERWAVE-MCAS','FLUTTERWAVE') then 
              if(cast(amount*0.005 as decimal(10,2)) > 70000, 70000, cast(amount*0.005 as decimal(10,2)))
              
            when order_status = 'SUCCESS' and out_channel_id='PAYSTACK' then 
              if(cast(amount*0.008 as decimal(10,2)) > 200000, 200000, cast(amount*0.008 as decimal(10,2)))
              
            when order_status = 'SUCCESS' and out_channel_id='INTERSWITCH' then 
              if(cast(amount*0.0075 as decimal(10,2)) > 120000, 120000, cast(amount*0.0075 as decimal(10,2)))
              
            when order_status = 'SUCCESS' and out_channel_id='UbaBank' then 
              if(cast(amount*0.0065 as decimal(10,2)) > 150000, 150000, cast(amount*0.0065 as decimal(10,2)))
              
            when order_status = 'SUCCESS' and out_channel_id='GTGIPS' then 
              if(cast(amount*0.0075 as decimal(10,2)) > 100000, 100000, cast(amount*0.0075 as decimal(10,2)))

            else 0
        end as channel_amount,
        'NG' as country_code,
        '{pt}' dt
        
    from (
        select 
            order_no, amount, currency, 'USER' as originator_type, user_id as originator_id, 
            bind_card_id as affiliate_bind_card_id, bank_code as affiliate_bank_code, bank_name as affiliate_bank_name, 
            bank_account_no_encrypted as affiliate_bank_account_no_encrypted, bank_card_no_encrypted as affiliate_bank_card_no_encrypted,
             '-' as affiliate_bank_mobile, '-' as affiliate_bank_email, scheme as affiliate_bank_scheme, '-' as payment_order_no,
            default.localTime("{config}", 'NG',create_time, 0) as create_time,
            default.localTime("{config}", 'NG',update_time, 0) as update_time, 
            country, order_status, error_code, error_msg, client_source, pay_channel as pay_way, next_step, out_channel_id, business_type, accounting_status,
            nvl(fee, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, nvl(outward_id, '-') as outward_id, nvl(outward_type, '-') as outward_type
        from opay_dw_ods.ods_sqoop_base_user_topup_record_di
        where dt = '{pt}'
        union all
        select 
            order_no, amount, currency, 'MERCHANT' as originator_type, merchant_id as originator_id, 
            '-' as affiliate_bind_card_id, bank_code as affiliate_bank_code, bank_name as affiliate_bank_name, 
            bank_account_no_encrypted as affiliate_bank_account_no_encrypted, bank_card_no_encrypted as affiliate_bank_card_no_encrypted,
            customer_phone as affiliate_bank_mobile, customer_email as affiliate_bank_email, scheme as affiliate_bank_scheme, merchant_order_no as payment_order_no,
            default.localTime("{config}", 'NG',create_time, 0) as create_time,
            default.localTime("{config}", 'NG',update_time, 0) as update_time, 
            country, order_status, error_code, error_msg, '-' as client_source, pay_channel as pay_way, next_step, out_channel_id, '-' as business_type, accounting_status,
            nvl(fee, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, nvl(outward_id, '-') as outward_id, nvl(outward_type, '-') as outward_type
        from opay_dw_ods.ods_sqoop_base_merchant_topup_record_di
        where dt = '{pt}'
    ) t1 
    left join dim_user_merchant_data t2 on t1.originator_id = t2.trader_id
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name,
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_topup_with_card_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_opay_topup_with_card_record_di_task = PythonOperator(
    task_id='dwd_opay_topup_with_card_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task >> dwd_opay_topup_with_card_record_di_task
ods_sqoop_base_merchant_df_prev_day_task >> dwd_opay_topup_with_card_record_di_task
ods_sqoop_base_user_topup_record_di_prev_day_task >> dwd_opay_topup_with_card_record_di_task
ods_sqoop_base_merchant_topup_record_di_prev_day_task >> dwd_opay_topup_with_card_record_di_task
