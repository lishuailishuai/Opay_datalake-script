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
    'start_date': datetime(2019, 12, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opay_account_balance_df',
                 schedule_interval="30 02 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##
ods_sqoop_base_account_user_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_account_user_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_account/account_user",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_account_merchant_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_account_merchant_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_account/account_merchant",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

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
ods_sqoop_owealth_share_acct_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_owealth_share_acct_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_owealth_ods/opay_owealth/share_acct",
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
db_name="opay_dw"

table_name = "dwd_opay_account_balance_df"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))

def dwd_opay_account_balance_df_sql_task(ds):
    HQL = '''
    
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    
    with 
        um_data as (
            SELECT 
                user_id,
                business_name,
                ROLE,
                kyc_level,
                category,
                '' merchant_type,
                'NG' AS country_code,
                 mobile
            FROM (
                SELECT 
                    user_id,
                    business_name,
                    ROLE,
                    kyc_level,
                    '' category,mobile,
                    row_number()over(partition BY user_id ORDER BY update_time DESC) rn,
                    country
                FROM opay_dw_ods.ods_sqoop_base_user_di where dt<='{pt}') m
                WHERE rn=1
            union all
            SELECT 
                    merchant_id AS user_id,
                    '' business_name,
                    'merchant'AS ROLE,
                    '' kyc_level,
                    category,
                    merchant_type,
                    'NG' AS country_code,
                        '-' as mobile
                FROM opay_dw_ods.ods_sqoop_base_merchant_df
                WHERE dt='{pt}' AND create_time<'{pt} 23:00:00'
        )
       
    insert overwrite table {db}.{table} partition (country_code,dt)
    select
        t0.user_id,
        user_type,
        nvl(business_name, '-'),
        nvl(ROLE, '-'),
        nvl(kyc_level, '-'),
        nvl(account_type, '-'),
        nvl(category, '-'),
        nvl(account_no, '-'),
        nvl(balance, 0),
        nvl(currency, '-'),
        nvl(account_state, '-'),
        nvl(auth, '-'),
        nvl(create_time, '-'),
        nvl(update_time, '-'),
        nvl(merchant_type, '-'),
        'NG' as country_code,
        '{pt}' dt
    from (
        SELECT 
            merchant_id AS user_id,
            'MERCHANT' user_type,
            account_type,
            account_no,
            balance,
            currency,
            account_state,
            auth,
            default.localTime("{config}", 'NG',create_time, 0) as create_time,
            default.localTime("{config}", 'NG',update_time, 0) as update_time
        FROM opay_dw_ods.ods_sqoop_base_account_merchant_df
        where dt='{pt}' and create_time<'{pt} 23:00:00'
        union all
        SELECT 
            user_id,
            'USER' user_type,
            account_type,
            account_no,
            balance,
            currency,
            account_state,
            auth,
            default.localTime("{config}", 'NG',create_time, 0) as create_time,
            default.localTime("{config}", 'NG',update_time, 0) as update_time
        FROM opay_dw_ods.ods_sqoop_base_account_user_df where dt='{pt}' AND create_time<'{pt} 23:00:00'
    ) t0 inner join um_data t1 on t0.user_id = t1.user_id
    union all
    SELECT 
        t3.user_id,
        user_type,
        nvl(business_name, '-'),
        nvl(ROLE, '-'),
        nvl(kyc_level, '-'),
        nvl(account_type, '-'),
        nvl(category, '-'),
        nvl(account_no, '-'),
        nvl(balance, 0),
        nvl(currency, '-'),
        nvl(account_state, '-'),
        nvl(auth, '-'),
        nvl(create_time, '-'),
        nvl(update_time, '-'),
        nvl(merchant_type, '-'),
        nvl(country_code, '-'),
        '{pt}'  dt 
    from (
         SELECT 
            user_id,'USER' user_type,'OWEALTH' account_type,share_acct_id account_no,balance,currency,acct_status account_state,memo auth,
            create_time,
            update_time
        FROM opay_owealth_ods.ods_sqoop_owealth_share_acct_df 
        where dt='{pt}' and date_format(create_time, 'yyyy-MM-dd') <= '{pt}'
    ) t2 inner join um_data t3 on t2.user_id = t3.mobile
   
   
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_opay_account_balance_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_opay_account_balance_df_task = PythonOperator(
    task_id='dwd_opay_account_balance_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

ods_sqoop_base_account_user_df_prev_day_task >> dwd_opay_account_balance_df_task
ods_sqoop_base_account_merchant_df_prev_day_task >> dwd_opay_account_balance_df_task
ods_sqoop_base_user_di_prev_day_task >> dwd_opay_account_balance_df_task
ods_sqoop_base_merchant_df_prev_day_task >> dwd_opay_account_balance_df_task
ods_sqoop_owealth_share_acct_df_prev_day_task >> dwd_opay_account_balance_df_task