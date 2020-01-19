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

args = {
    'owner': 'liushuzhen',
    'start_date': datetime(2020, 1, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_active_user_report_d',
                  schedule_interval="40 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dim_opay_user_base_di_prev_day_task = OssSensor(
    task_id='dim_opay_user_base_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_user_base_di/country_code=NG",
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

ods_sqoop_base_user_payment_instrument_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_payment_instrument_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_user/user_payment_instrument",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_user_operator_df_prev_day_task = OssSensor(
    task_id='ods_sqoop_base_user_operator_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_user/user_operator",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
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
##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"

table_name = "app_opay_active_user_report_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_active_user_report_d_sql_task(ds):
    HQL = '''

    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    DROP TABLE IF EXISTS test_db.user_base;
    DROP TABLE IF EXISTS test_db.opay_30d;
    DROP TABLE IF EXISTS test_db.login;
    create table test_db.user_base_{pt} as 
    SELECT user_id, ROLE,mobile
     FROM
          (SELECT user_id, ROLE,mobile, row_number() over(partition BY user_id ORDER BY update_time DESC) rn
            FROM opay_dw.dim_opay_user_base_di
           WHERE dt<='{pt}' ) t1
     WHERE rn = 1;
       
    create table test_db.opay_30d_{pt} as 
      select user_id,
             dt  
      from opay_dw_ods.ods_sqoop_base_account_user_df 
      where balance>0 and account_type='CASHACCOUNT' and dt>date_sub('{pt}',30) and dt<='{pt}' and create_time<'{pt} 23:00:00'
      union 
      select b.user_id,
             dt
      from 
        (select user_id,
                dt 
         from opay_owealth_ods.ods_sqoop_owealth_share_acct_df where balance>0 and dt>date_sub('{pt}',30) and dt<='{pt}' and create_time<'{pt} 23:00:00')a 
      inner join 
         test_db.user_base_{pt} b on a.user_id=b.mobile;
               
    create table test_db.login_{pt} as 
    select dt,
           substr(from_unixtime(unix_timestamp(last_visit, 'yyyy-MM-dd HH:mm:ss')+3600),1,10) last_visit,a.user_id,role 
     from 
          (select dt,user_id ,last_visit
             from opay_dw_ods.ods_sqoop_base_user_operator_df 
             where dt='{pt}' and substr(from_unixtime(unix_timestamp(last_visit, 'yyyy-MM-dd HH:mm:ss')+3600),1,10) > date_sub('{pt}',30))a 
    inner join 
          test_db.user_base_{pt} b 
    on a.user_id=b.user_id;
    
    with bind_card as
       (SELECt dt,
               user_id,
               pay_status
       FROM opay_dw_ods.ods_sqoop_base_user_payment_instrument_df
       WHERE dt='{pt}'
       AND create_time<'{pt} 23:00:00'
       AND payment_type = '1'
       ),
      opay_account as
       (select user_id,
              'owellet' flag,
               dt 
       from opay_dw_ods.ods_sqoop_base_account_user_df 
       where balance>0 and account_type='CASHACCOUNT' and dt='{pt}' and create_time<'{pt} 23:00:00'
       union 
       select b.user_id ,
              'owealth' flag,
              dt
       from 
            (select user_id,dt from opay_owealth_ods.ods_sqoop_owealth_share_acct_df where balance>0 and dt='{pt}' and create_time<'{pt} 23:00:00')a 
       inner join 
           test_db.user_base_{pt} b on a.user_id=b.mobile
         
       ),
    
      opay_active as 
           (select a.user_id,
                   a.dt 
            from 
               (select dt,user_id from opay_account union all select dt,user_id from bind_card) a
            inner join 
               (select user_id from test_db.login where last_visit<='{pt}') b 
            on a.user_id=b.user_id)
    
    INSERT overwrite TABLE opay_dw.app_opay_active_user_report_d partition (dt,target_type)
    
    SELECT 
                '-' country_code,
                '-' city,
                ROLE,
                '-' kyc_level,
                top_consume_scenario,
                '-' register_client,
                c,
                dt,
                target_type
            FROM (
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'bind_card_user_cnt' target_type,
                    count(DISTINCT user_id ) c
                FROM bind_card
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'bind_card_pay_user_cnt' target_type,
                    count(DISTINCT CASE WHEN pay_status='1' THEN user_id END) c
                FROM bind_card
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_d' target_type,
                    count(DISTINCT CASE WHEN last_visit='{pt}' THEN user_id END) c
                FROM test_db.login_{pt}
                GROUP BY dt, ROLE
                UNION ALL 
                SELECT 
                    dt,
                    'ALL' ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_d' target_type,
                    count(DISTINCT CASE WHEN last_visit='{pt}' THEN user_id END) c
                FROM test_db.login_{pt}
                GROUP BY dt
                UNION 
                ALL 
                SELECT 
                    dt,
                    ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_7d' target_type,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',7) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{pt}
                GROUP BY dt, ROLE
                UNION ALL 
                SELECT 
                    dt,
                    'ALL' ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_7d' target_type,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',7) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{pt}
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_30d' target_type,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',30) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{pt}
                GROUP BY dt, ROLE
                UNION ALL 
                SELECT 
                    dt,
                    'ALL' ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_30d' target_type,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',30) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{pt}
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'owallet_bal_not_zero_user_cnt' target_type,
                    count(DISTINCT user_id) c
                FROM opay_dw_ods.ods_sqoop_base_account_user_df
                WHERE dt='{pt}' AND account_type='CASHACCOUNT' AND balance>0 and create_time<'{pt} 23:00:00'
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'owealth_bal_not_zero_user_cnt' target_type,
                    count(DISTINCT user_id) c
                FROM opay_owealth_ods.ods_sqoop_owealth_share_acct_df
                WHERE dt='{pt}' AND balance>0 and create_time<'{pt} 23:00:00'
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt' target_type,
                    count(DISTINCT user_id) c
                FROM opay_account
                GROUP BY dt
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt_7d' target_type,
                    count(1) c
                from 
                   (select user_id FROM test_db.opay_30d_{pt} where dt>=date_sub('{pt}',7) group by user_id) m
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt_30d' target_type,
                    count(1) c
                FROM 
                   (select user_id from test_db.opay_30d_{pt} group by user_id)m 
                UNION ALL 
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_active_user_cnt' target_type,
                    count(DISTINCT user_id) c
                FROM opay_active
                GROUP BY dt
            ) m;
    DROP TABLE IF EXISTS test_db.user_base_{pt};
    DROP TABLE IF EXISTS test_db.opay_30d_{pt};
    DROP TABLE IF EXISTS test_db.login_{pt};
        



    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_active_user_report_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_active_user_report_d_task = PythonOperator(
    task_id='app_opay_active_user_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opay_user_base_di_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_user_payment_instrument_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_user_operator_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_owealth_share_acct_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_account_user_df_prev_day_task >> app_opay_active_user_report_d_task


