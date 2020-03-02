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
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
from utils.get_local_time import GetLocalTime


args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2020, 3, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opay_cash_to_card_record_hi',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  catchup=False)


##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "dwd_opay_cash_to_card_record_hi"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))
time_zone = config['NG']['time_zone']
##----------------------------------------- 依赖 ---------------------------------------##
### 检查最新的用户表的依赖
dim_opay_user_base_hf_check_task = OssSensor(
    task_id='dim_opay_user_base_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_user_base_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查最新的商户表的依赖
dim_opay_merchant_base_hf_check_task = OssSensor(
    task_id='dim_opay_merchant_base_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_merchant_base_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
### 检查当前小时的分区依赖
ods_binlog_base_user_transfer_card_record_hi_check_task = OssSensor(
    task_id='ods_binlog_base_user_transfer_card_record_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_transaction.opay_transaction.user_transfer_card_record",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_binlog_base_merchant_transfer_card_record_hi_check_task = OssSensor(
    task_id='ods_binlog_base_merchant_transfer_card_record_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_transaction.opay_transaction.merchant_transfer_card_record",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



##----------------------------------------- 任务超时监控 ---------------------------------------##



def dwd_opay_cash_to_card_record_hi_sql_task(ds,v_date):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    
    with 
    dim_user_merchant_data as (
      select 
        user_id as trader_id
        , concat(first_name, ' ', middle_name, ' ', surname) as trader_name
        , `role` as trader_role
        , kyc_level as trader_kyc_level
        , state
      from 
        opay_dw.dim_opay_user_base_hf
      where 
        concat(dt,' ',lpad(hour,2,'0')) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
        and concat(dt,' ',lpad(hour,2,'0')) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
        and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
    
      union all
    
      select 
        merchant_id as trader_id
        , merchant_name as trader_name
        , merchant_type as trader_role
        , '-' as trader_kyc_level
        , '-' as state
      from 
        opay_dw.dim_opay_merchant_base_hf
      where 
        concat(dt,' ',lpad(hour,2,'0')) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
        and concat(dt,' ',lpad(hour,2,'0')) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
        and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
    )
    
    insert overwrite table {db}.{table} 
    partition(country_code, dt,hour)

    select 
        order_no, amount, currency, originator_type, t2.trader_role as originator_role, t2.trader_kyc_level as originator_kyc_level, t1.originator_id, t2.trader_name as originator_name,
        t1.affiliate_bank_code, t1.affiliate_bank_name, t1.affiliate_bank_account_no_encrypted, t1.affiliate_bank_account_name, t1.affiliate_bank_kyc_level, t1.affiliate_bank_mobile, 
        t1.affiliate_bank_email, t1.create_time, t1.update_time, t1.country, 'Cash to Card' as top_service_type, 'ACTransfer' as sub_service_type, 
        t1.order_status, t1.error_code, t1.error_msg, t1.client_source, t1.pay_way, t1.business_type,
        t1.pay_status, 'ACTransfer' as top_consume_scenario, 'ACTransfer' as sub_consume_scenario,
        t1.fee_amount, t1.fee_pattern, t1.outward_id, t1.outward_type,t1.utc_date_hour,state,
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
        date_format(default.localTime("{config}", t1.country, '{v_date}', 0), 'yyyy-MM-dd') as dt,
        date_format(default.localTime("{config}", t1.country, '{v_date}', 0), 'HH') as hour

    from (
        select 
            order_no, amount, currency, 'USER' as originator_type, user_id as originator_id, 
            recipient_bank_code as affiliate_bank_code, recipient_bank_name as affiliate_bank_name, 
            recipient_bank_account_no_encrypted as affiliate_bank_account_no_encrypted, recipient_bank_account_name as affiliate_bank_account_name, 
            recipient_kyc_level as affiliate_bank_kyc_level, '-' as affiliate_bank_mobile, '-' as affiliate_bank_email,
            default.localTime("{config}",if(nvl(country,'')='','NG',country),from_unixtime(cast(cast(create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as create_time, 
            default.localTime("{config}",if(nvl(country,'')='','NG',country),from_unixtime(cast(cast(update_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as update_time, 
            country, order_status, error_code, error_msg, client_source, pay_channel as pay_way, business_type, pay_status,
            nvl(fee, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, nvl(outward_id, '-') as outward_id, nvl(outward_type, '-') as outward_type,
            date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour
        from 
           (select *,row_number() over(partition by order_no order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
             from opay_dw_ods.ods_binlog_base_user_transfer_card_record_hi
            where 
                dt = date_format('{v_date}', 'yyyy-MM-dd')
                and hour= date_format('{v_date}', 'HH')
                and `__deleted` = 'false') m
        where rn=1
        union all
        select 
            order_no, amount, currency, 'MERCHANT' as originator_type, merchant_id as originator_id, 
            recipient_bank_code as affiliate_bank_code, recipient_bank_name as affiliate_bank_name, 
            recipient_bank_account_no_encrypted as affiliate_bank_account_no_encrypted, recipient_bank_account_name as affiliate_bank_account_name, 
            recipient_kyc_level as affiliate_bank_kyc_level, customer_phone as affiliate_bank_mobile, customer_email as affiliate_bank_email,
            default.localTime("{config}",if(nvl(country,'')='','NG',country),from_unixtime(cast(cast(create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as create_time, 
            default.localTime("{config}",if(nvl(country,'')='','NG',country),from_unixtime(cast(cast(update_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as update_time, 
            country, order_status, error_code, error_msg, '-' as client_source, pay_channel as pay_way, business_type, pay_status,
            nvl(fee, 0) as fee_amount, nvl(fee_pattern, '-') as fee_pattern, nvl(outward_id, '-') as outward_id, nvl(outward_type, '-') as outward_type,
            date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour
        from (select *,row_number() over(partition by order_no order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
               from opay_dw_ods.ods_binlog_base_merchant_transfer_card_record_hi
                where 
                dt = date_format('{v_date}', 'yyyy-MM-dd')
                and hour= date_format('{v_date}', 'HH')
                and `__deleted` = 'false')m
        where rn=1

    ) t1 
    left join dim_user_merchant_data t2 on t1.originator_id = t2.trader_id
    '''.format(
        pt=ds,
        v_date=v_date,
        table=table_name,
        db=db_name,
        config=config
    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
            alter语句: alter_partition()
            删除分区: delete_partition()
            生产success: touchz_success()

        #参数
            is_countries_online --是否开通多国家业务 默认(true 开通)
            db_name --hive 数据库的名称
            table_name --hive 表的名称
            data_oss_path --oss 数据目录的地址
            is_country_partition --是否有国家码分区,[默认(true 有country_code分区)]
            is_result_force_exist --数据是否强行产出,[默认(true 必须有数据才生成_SUCCESS)] false 数据没有也生成_SUCCESS 
            execute_time --当前脚本执行时间(%Y-%m-%d %H:%M:%S)
            is_hour_task --是否开通小时级任务,[默认(false)]
            frame_type --模板类型(只有 is_hour_task:'true' 时生效): utc 产出分区为utc时间，local 产出分区为本地时间,[默认(utc)]。

        #读取sql
            %_sql(ds,v_hour)

    """

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "true",
            "frame_type": "local"
        }
    ]

    cf = CountriesPublicFrame_dev(args)

   # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_cash_to_card_record_hi_sql_task(ds, v_date)


    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生产success
    cf.touchz_success()


dwd_opay_cico_record_hi_task = PythonOperator(
    task_id='dwd_opay_cico_record_hi_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

#dim_opay_user_base_hf_check_task >> dwd_opay_cash_to_card_record_hi_sql_task
#dim_opay_merchant_base_hf_check_task >> dwd_opay_cash_to_card_record_hi_sql_task
#ods_binlog_base_user_transfer_card_record_hi_check_task >> dwd_opay_cash_to_card_record_hi_sql_task
#ods_binlog_base_merchant_transfer_card_record_hi_check_task >> dwd_opay_cash_to_card_record_hi_sql_task
