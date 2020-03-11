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
    'owner': 'yuanfeng',
    'start_date': datetime(2020, 2, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opay_transfer_of_account_record_hi',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "dwd_opay_transfer_of_account_record_hi"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
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

### 检查当前小时的分区依赖
ods_binlog_base_merchant_transfer_user_record_hi_check_task = OssSensor(
    task_id='ods_binlog_base_merchant_transfer_user_record_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_transaction_db.opay_transaction.merchant_transfer_user_record",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查当前小时的分区依赖
ods_binlog_base_merchant_acquiring_record_hi_check_task = OssSensor(
    task_id='ods_binlog_base_merchant_acquiring_record_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_transaction_db.opay_transaction.merchant_acquiring_record",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查当前小时的分区依赖
ods_binlog_base_user_transfer_user_record_hi_check_task = OssSensor(
    task_id='ods_binlog_base_user_transfer_user_record_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_transaction_db.opay_transaction.user_transfer_user_record",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, execution_date, **op_kwargs):
    dag_ids = dag.dag_id

    # 监控国家
    v_country_code = 'NG'

    # 时间偏移量
    v_gap_hour = 0

    v_date = GetLocalTime("opay", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['date']
    v_hour = GetLocalTime("opay", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['hour']

    # 小时级监控
    tb_hour_task = [
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code={country_code}/dt={pt}/hour={now_hour}".format(country_code=v_country_code,
                                                                                   pt=v_date, now_hour=v_hour),
         "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def dwd_opay_transfer_of_account_record_hi_sql_task(ds, v_date):
    HQL = '''


set mapred.max.split.size=1000000;
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
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
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
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

dim_service_scenario_data as (
  select 
    sub_service_type
    , top_consume_scenario
    , sub_consume_scenario
    , trader_id
  from 
    opay_dw.dim_opay_service_scenario_mapping_df 
  where 
    dt = date_add('{pt}',-1)
),

merchant_transfer_user_data as (
  select 
    order_no
    , amount
    , currency
    , originator_type
    , originator_id
    , affiliate_type
    , affiliate_id
    , payment_order_no
    , create_time
    , update_time
    , country
    , m1.sub_service_type as sub_service_type
    , order_status
    , error_code
    , error_msg
    , client_source
    , pay_way
    , business_type
    , case 
        when mp1.top_consume_scenario is null then m1.sub_service_type
        else mp1.top_consume_scenario
        end as top_consume_scenario
    , case 
        when mp1.sub_consume_scenario is null then m1.sub_service_type
        else mp1.sub_consume_scenario
        end as sub_consume_scenario
    , fee_amount
    , fee_pattern
    , outward_id
    , outward_type
    , utc_date_hour
  from 
    (
    select 
      order_no
      , amount
      , currency
      , 'MERCHANT' as originator_type
      , merchant_id as originator_id
      , recipient_type as affiliate_type
      , recipient_id as affiliate_id
      , merchant_order_no as payment_order_no
      , default.localTime("{config}",'NG',from_unixtime(cast(cast(create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as create_time
      , default.localTime("{config}",'NG',from_unixtime(cast(cast(update_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as update_time
      , 'NG' as country
      , 'AATransfer' as sub_service_type
      , order_status
      , '-' as error_code
      , error_msg
      , '-' as client_source
      , pay_channel as pay_way
      , business_type
      , nvl(fee_amount, 0) as fee_amount
      , nvl(fee_pattern, '-') as fee_pattern
      , nvl(outWardId, '-') as outward_id
      , nvl(outWardType, '-') as outward_type
      , date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

      , row_number() over(partition by order_no order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
    from 
      opay_dw_ods.ods_binlog_base_merchant_transfer_user_record_hi
    where 
      dt = date_format('{v_date}', 'yyyy-MM-dd')
      and hour= date_format('{v_date}', 'HH')
      and `__deleted` = 'false'
    ) m1 
  left join 
    dim_service_scenario_data mp1 
  on 
    m1.originator_id = mp1.trader_id 
    and m1.sub_service_type = mp1.sub_service_type
  where
    m1.rn = 1
),

merchant_acquiring_data as (
  select 
    m2.order_no as order_no
    , m2.amount as amount
    , m2.currency as currency
    , m2.originator_type as originator_type
    , m2.originator_id as originator_id
    , m2.affiliate_type as affiliate_type
    , m2.affiliate_id as affiliate_id
    , m2.payment_order_no as payment_order_no
    , m2.create_time as create_time
    , m2.update_time as update_time
    , m2.country as country
    , m2.sub_service_type as sub_service_type
    , m2.order_status as order_status
    , m2.error_code as error_code
    , m2.error_msg as error_msg
    , m2.client_source as client_source
    , m2.pay_way as pay_way
    , m2.business_type as business_type
    , case 
        when mp2.top_consume_scenario is null then m2.sub_service_type
        else mp2.top_consume_scenario
        end as top_consume_scenario
    , case 
        when mp2.sub_consume_scenario is null then m2.sub_service_type
        else mp2.sub_consume_scenario
        end as sub_consume_scenario
    , fee_amount
    , fee_pattern
    , outward_id
    , outward_type
    , utc_date_hour
  from 
    (
    select 
      order_no
      , amount
      , currency
      , 'USER' as originator_type
      , user_id as originator_id
      , 'MERCHANT' as affiliate_type
      , merchant_id as affiliate_id
      , merchant_order_no as payment_order_no
      , default.localTime("{config}",'NG',from_unixtime(cast(cast(create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as create_time
      , default.localTime("{config}",'NG',from_unixtime(cast(cast(update_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as update_time
      , 'NG' as country
      , 'MAcquiring' as sub_service_type
      , order_status
      , '-' as error_code
      , error_msg
      , '-' as client_source
      , pay_channel as pay_way
      , bussiness_type as business_type
      , nvl(fee, 0) as fee_amount
      , nvl(fee_pattern, '-') as fee_pattern
      , nvl(outward_id, '-') as outward_id
      , nvl(outward_type, '-') as outward_type
      , date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

      , row_number() over(partition by order_no order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
    from 
      opay_dw_ods.ods_binlog_base_merchant_acquiring_record_hi
    where 
      dt = date_format('{v_date}', 'yyyy-MM-dd')
      and hour= date_format('{v_date}', 'HH')
      and `__deleted` = 'false'
    ) m2 
  left join 
    dim_service_scenario_data mp2 
  on 
    m2.affiliate_id = mp2.trader_id 
    and mp2.sub_service_type = m2.sub_service_type
  where
    m2.rn = 1
),

user_transfer_user_data as (
  select
    order_no
    ,amount
    ,currency
    ,originator_type
    ,originator_id
    ,affiliate_type
    ,affiliate_id
    ,payment_order_no
    ,create_time
    ,update_time
    ,country
    ,sub_service_type
    ,order_status
    ,error_code
    ,error_msg
    ,client_source
    ,pay_way
    ,business_type
    ,top_consume_scenario
    ,sub_consume_scenario
    ,fee_amount
    ,fee_pattern
    ,outward_id
    ,outward_type
    ,utc_date_hour
  from
    (
    select 
      order_no
      , amount
      , currency
      , 'USER' as originator_type
      , user_id as originator_id
      , recipient_type as affiliate_type
      , recipient_id as affiliate_id
      , '-' as payment_order_no
      , default.localTime("{config}",'NG',from_unixtime(cast(cast(create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as create_time
      , default.localTime("{config}",'NG',from_unixtime(cast(cast(update_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss'),0) as update_time
      , 'NG' as country
      , 'AATransfer' as sub_service_type
      , case transfer_status
        when 'CONFIRM_S' then 'SUCCESS'
        when 'TRANSFER_S' then 'SUCCESS'
        when 'TRANSFER_F' then 'FAIL'
        when 'FREEZE_F' then 'FAIL'
        when 'FREEZE_S' then 'PENDING'
        when 'TRANSFER_P' then 'PENDING'
        when 'FREEZE_P' then 'PENDING'
        when 'UNFREEZ_P' then 'PENDING'
        when 'CONFIRM_P' then 'PENDING'
        when 'UNFREEZ_S' then 'FAIL'
        end as order_status
      , '-' as error_code
      , error_msg
      , client_source
      , pay_channel as pay_way
      , business_type
      , 'AATransfer' as top_consume_scenario
      , 'AATransfer' as sub_consume_scenario
      , nvl(fee_amount, 0) as fee_amount
      , nvl(fee_pattern, '-') as fee_pattern
      , nvl(outWardId, '-') as outward_id
      , nvl(outWardType, '-') as outward_type
      , date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour
  
      , row_number() over(partition by order_no order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
    from 
      opay_dw_ods.ods_binlog_base_user_transfer_user_record_hi
    where 
      dt = date_format('{v_date}', 'yyyy-MM-dd')
      and hour= date_format('{v_date}', 'HH')
      and `__deleted` = 'false'
    ) as a
  where
    a.rn = 1
),

union_result as  (
  select * from merchant_transfer_user_data
  union all
  select * from merchant_acquiring_data
  union all
  select * from user_transfer_user_data
)

insert overwrite table opay_dw.dwd_opay_transfer_of_account_record_hi partition(country_code, dt, hour)
select 
  t1.order_no
  , t1.amount
  , t1.currency
  , t1.originator_type
  , t2.trader_role as originator_role
  , t2.trader_kyc_level as originator_kyc_level
  , t1.originator_id
  , t2.trader_name as originator_name
  , t1.affiliate_type
  , t3.trader_role as affiliate_role
  , t1.affiliate_id
  , t3.trader_name as affiliate_name
  , case 
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
    end as payment_relation_id
  , t1.payment_order_no
  , t1.create_time
  , t1.update_time
  , t1.country
  , 'Transfer of Account' as top_service_type
  , t1.sub_service_type
  , t1.order_status
  , t1.error_code
  , t1.error_msg
  , t1.client_source
  , t1.pay_way
  , t1.business_type
  , t1.top_consume_scenario
  , t1.sub_consume_scenario
  , t1.fee_amount
  , t1.fee_pattern
  , t1.outward_id
  , t1.outward_type
  , t2.state
  , t1.utc_date_hour

  , 'NG' as country_code
  , date_format(default.localTime("{config}", t1.country, '{v_date}', 0), 'yyyy-MM-dd') as dt
  , date_format(default.localTime("{config}", t1.country, '{v_date}', 0), 'HH') as hour
from 
  union_result as  t1 
left join 
  dim_user_merchant_data t2 
on 
  t1.originator_id = t2.trader_id
left join 
  dim_user_merchant_data t3 
on 
  t1.affiliate_id = t3.trader_id;




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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_transfer_of_account_record_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_opay_transfer_of_account_record_hi_task = PythonOperator(
    task_id='dwd_opay_transfer_of_account_record_hi_task',
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

dim_opay_merchant_base_hf_check_task >> dwd_opay_transfer_of_account_record_hi_task
dim_opay_user_base_hf_check_task >> dwd_opay_transfer_of_account_record_hi_task
ods_binlog_base_merchant_transfer_user_record_hi_check_task >> dwd_opay_transfer_of_account_record_hi_task
ods_binlog_base_merchant_acquiring_record_hi_check_task >> dwd_opay_transfer_of_account_record_hi_task
ods_binlog_base_user_transfer_user_record_hi_check_task >> dwd_opay_transfer_of_account_record_hi_task

