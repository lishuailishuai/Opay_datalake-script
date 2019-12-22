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

##
# 央行月报汇报指标
#
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


dag = airflow.DAG('dwd_opay_life_payment_record_di',
                  schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

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

ods_sqoop_base_betting_topup_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_betting_topup_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/betting_topup_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_tv_topup_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_tv_topup_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/tv_topup_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_electricity_topup_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_electricity_topup_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/electricity_topup_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_airtime_topup_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_airtime_topup_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/airtime_topup_record",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_mobiledata_topup_record_di_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_mobiledata_topup_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/mobiledata_topup_record",
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
table_name = "dwd_opay_life_payment_record_di"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name


def dwd_opay_life_payment_record_di_sql_task(ds):
    HQL='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with dim_user_data as (
            select 
                user_id, `role`, kyc_level, first_name, middle_name, surname
            from (
                select 
                    user_id, `role`, kyc_level, first_name, middle_name, surname,
                    row_number() over(partition by user_id order by update_time desc) rn
                from opay_dw_ods.ods_sqoop_base_user_di
                where dt <= '{pt}'
            ) uf where rn = 1
        ),
        dim_merchant_data as (
            select 
                merchant_id, merchant_name, merchant_type
            from opay_dw_ods.ods_sqoop_base_merchant_df
            where dt = if('{pt}' <= '2019-12-11', '2019-12-11', '{pt}')
        )
    insert overwrite table {db}.{table} 
    partition(country_code, dt)
    
    select 
        t1.order_no, t1.amount, t1.currency, 
        'USER' as originator_type, t2.role as originator_role, t2.kyc_level as originator_kyc_level, t1.originator_id, concat(t2.first_name, ' ', t2.middle_name, ' ', t2.surname) as originator_name,
        'MERCHANT' as affiliate_type, t3.merchant_type as affiliate_role, t3.merchant_id as affiliate_id, t3.merchant_name as affiliate_name,
        t1.recharge_service_provider, replace(t1.recharge_account, '+234', '') as recharge_account, t1.recharge_account_name, t1.recharge_set_meal,
        t1.create_time, t1.update_time, t1.country, 'Life Payment' as top_service_type, t1.sub_service_type,
        t1.order_status, t1.error_code, t1.error_msg, t1.client_source, t1.pay_way,
        case t1.country
            when 'NG' then 'NG'
            when 'NO' then 'NO'
            when 'GH' then 'GH'
            when 'BW' then 'BW'
            when 'GH' then 'GH'
            when 'KE' then 'KE'
            when 'MW' then 'MW'
            when 'MZ' then 'MZ'
            when 'PL' then 'PL'
            when 'ZA' then 'ZA'
            when 'SE' then 'SE'
            when 'TZ' then 'TZ'
            when 'UG' then 'UG'
            when 'US' then 'US'
            when 'ZM' then 'ZM'
            when 'ZW' then 'ZW'
            else 'NG'
            end as country_code,
        '{pt}' dt
        
    from (
        select 
            order_no, amount, currency, user_id as originator_id, 
            merchant_id as affiliate_id, 
            tv_provider as recharge_service_provider, recipient_tv_account_no as recharge_account, recipient_tv_account_name as recharge_account_name, tv_plan as recharge_set_meal,
            create_time, update_time, country, 'TV' sub_service_type, 
            order_status, error_code, error_msg, client_source, pay_channel as pay_way
        from opay_dw_ods.ods_sqoop_base_tv_topup_record_di
        where dt = '{pt}'
        union all
        select 
            order_no, amount, currency, user_id as originator_id,
            merchant_id as affiliate_id, 
            betting_provider as recharge_service_provider, recipient_betting_account as recharge_account, recipient_betting_name as recharge_account_name, '-' as recharge_set_meal,
            create_time, update_time, country, 'Betting' sub_service_type,
            order_status, error_code, error_msg, client_source, pay_channel as pay_way
        from opay_dw_ods.ods_sqoop_base_betting_topup_record_di
        where dt = '{pt}' and betting_provider != '' and betting_provider != 'supabet' and betting_provider is not null
        union all
        select 
            order_no, amount, currency, user_id as originator_id,
            merchant_id as affiliate_id, 
            telecom_perator as recharge_service_provider, recipient_mobile as recharge_account, '-' as recharge_account_name, '-' as recharge_set_meal,
            create_time, update_time, country, 'Mobiledata' sub_service_type,
            order_status, error_code, error_msg, client_source, pay_channel as pay_way
        from opay_dw_ods.ods_sqoop_base_mobiledata_topup_record_di
        where dt = '{pt}'   
        union all
        select 
            order_no, amount, currency, user_id as originator_id,
            merchant_id as affiliate_id,
            telecom_perator as recharge_service_provider, recipient_mobile as recharge_account, '-' as recharge_account_name, '-' as recharge_set_meal,
            create_time, update_time, country, 'Airtime' sub_service_type,
            order_status, error_code, error_msg, client_source, pay_channel as pay_way
        from opay_dw_ods.ods_sqoop_base_airtime_topup_record_di
        where dt = '{pt}' 
        union all
        select 
            order_no, amount, currency, user_id as originator_id,
            merchant_id as affiliate_id,
            recipient_elec_perator as recharge_service_provider, recipient_elec_account as recharge_account, '-' as recharge_account_name, electricity_payment_plan as recharge_set_meal,
            create_time, update_time, country, 'Electricity' sub_service_type,
            order_status, error_code, error_msg, client_source, pay_channel as pay_way
        from opay_dw_ods.ods_sqoop_base_electricity_topup_record_di
        where dt = '{pt}'
    ) t1 
    left join dim_user_data t2 on t1.originator_id = t2.user_id
    left join dim_merchant_data t3 on t1.affiliate_id = t3.merchant_id
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
    _sql = dwd_opay_life_payment_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)


    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_opay_life_payment_record_di_task = PythonOperator(
    task_id='dwd_opay_life_payment_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task >> dwd_opay_life_payment_record_di_task
ods_sqoop_base_merchant_df_prev_day_task >> dwd_opay_life_payment_record_di_task
ods_sqoop_base_electricity_topup_record_di_prev_day_task >> dwd_opay_life_payment_record_di_task
ods_sqoop_base_airtime_topup_record_di_prev_day_task >> dwd_opay_life_payment_record_di_task
ods_sqoop_base_tv_topup_record_di_prev_day_task >> dwd_opay_life_payment_record_di_task
ods_sqoop_base_mobiledata_topup_record_di_prev_day_task >> dwd_opay_life_payment_record_di_task
ods_sqoop_base_betting_topup_record_di_prev_day_task >> dwd_opay_life_payment_record_di_task