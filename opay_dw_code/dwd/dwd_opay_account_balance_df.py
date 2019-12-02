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

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2019, 12, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opay_account_balance_df',
                 schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##
ods_sqoop_base_account_user_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_account_user_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_ods/opay_account/account_user",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_account_merchant_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_account_merchant_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_ods/opay_account/account_merchant",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_user_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_user/user",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_merchant_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_merchant_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_ods/opay_merchant/merchant",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name="opay_dw"

table_name = "dwd_opay_account_balance_df"
hdfs_path="ufile://opay-datalake/opay/opay_dw/" + table_name

def dwd_opay_account_balance_df_sql_task(ds):
    HQL = '''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table {db}.{table} partition (country_code,dt)
    SELECT a.user_id,
           user_type,
           business_name,
           ROLE,
           kyc_level,
           account_type,
           category,
           account_no,
           balance,
           currency,
           account_state,
           auth,
           create_time,
           update_time,
           merchant_type,
           country_code,
           '{pt}'           
    FROM
      (SELECT user_id,
              'USER' user_type,
                     account_type,
                     account_no,
                     balance,
                     currency,
                     account_state,
                     auth,
                     create_time,
                     update_time
       FROM opay_dw_ods.ods_sqoop_base_account_user_df where dt='{pt}'
            AND create_time<'{pt} 23:00:00'
       UNION ALL SELECT merchant_id AS user_id,
                        'MERCHANT' user_type,
                                   account_type,
                                   account_no,
                                   balance,
                                   currency,
                                   account_state,
                                   auth,
                                   create_time,
                                   update_time
       FROM opay_dw_ods.ods_sqoop_base_account_merchant_df where dt='{pt}'
            AND create_time<'{pt} 23:00:00') a
    INNER JOIN
      (SELECT user_id,
              business_name,
              ROLE,
              kyc_level,
              category,
              '' merchant_type,
                 CASE country
                     WHEN 'NG' THEN 'NG'
                     WHEN 'NO' THEN 'NO'
                     WHEN 'GH' THEN 'GH'
                     WHEN 'BW' THEN 'BW'
                     WHEN 'GH' THEN 'GH'
                     WHEN 'KE' THEN 'KE'
                     WHEN 'MW' THEN 'MW'
                     WHEN 'MZ' THEN 'MZ'
                     WHEN 'PL' THEN 'PL'
                     WHEN 'ZA' THEN 'ZA'
                     WHEN 'SE' THEN 'SE'
                     WHEN 'TZ' THEN 'TZ'
                     WHEN 'UG' THEN 'UG'
                     WHEN 'US' THEN 'US'
                     WHEN 'ZM' THEN 'ZM'
                     WHEN 'ZW' THEN 'ZW'
                     ELSE 'NG'
                 END AS country_code
       FROM
         (SELECT user_id,
                 business_name,
                 ROLE,
                 kyc_level,
                 '' category,
                    row_number()over(partition BY user_id
                                     ORDER BY update_time DESC) rn,
                                country
          FROM opay_dw_ods.ods_sqoop_base_user_di where create_time<'{pt} 23:00:00') m
       WHERE rn=1
       UNION ALL SELECT merchant_id AS user_id,
                        '' business_name,
                          'merchant'AS ROLE,
                          '' kyc_level,
                             category,
                             merchant_type,
                             CASE countries_code
                                 WHEN 'NG' THEN 'NG'
                                 WHEN 'NO' THEN 'NO'
                                 WHEN 'GH' THEN 'GH'
                                 WHEN 'BW' THEN 'BW'
                                 WHEN 'GH' THEN 'GH'
                                 WHEN 'KE' THEN 'KE'
                                 WHEN 'MW' THEN 'MW'
                                 WHEN 'MZ' THEN 'MZ'
                                 WHEN 'PL' THEN 'PL'
                                 WHEN 'ZA' THEN 'ZA'
                                 WHEN 'SE' THEN 'SE'
                                 WHEN 'TZ' THEN 'TZ'
                                 WHEN 'UG' THEN 'UG'
                                 WHEN 'US' THEN 'US'
                                 WHEN 'ZM' THEN 'ZM'
                                 WHEN 'ZW' THEN 'ZW'
                                 ELSE 'NG'
                             END AS country_code
       FROM opay_dw_ods.ods_sqoop_base_merchant_df
       WHERE dt='{pt}' AND create_time<'{pt} 23:00:00' )b ON a.user_id=b.user_id
   
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL

def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opay_account_balance_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_opay_account_balance_df_task = PythonOperator(
    task_id='dwd_opay_account_balance_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_account_user_df_prev_day_task >> dwd_opay_account_balance_df_task
ods_sqoop_base_account_merchant_df_prev_day_task >> dwd_opay_account_balance_df_task
ods_sqoop_base_user_di_prev_day_task >> dwd_opay_account_balance_df_task
ods_sqoop_base_merchant_df_prev_day_task >> dwd_opay_account_balance_df_task
