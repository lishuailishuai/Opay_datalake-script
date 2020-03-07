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
                  default_args=args)

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

dwm_opay_user_balance_df_prev_day_task = OssSensor(
    task_id='dwm_opay_user_balance_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_user_balance_df/country_code=NG",
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

table_name = "app_opay_active_user_report_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_active_user_report_d_sql_task(ds,ds_nodash):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
     DROP TABLE IF EXISTS test_db.user_base_{date};
    DROP TABLE IF EXISTS test_db.login_{date};
    DROP TABLE IF EXISTS test_db.tran_{date};
    create table if not exists test_db.user_base_{date} as 
    SELECT user_id, ROLE,mobile,state
     FROM
          (SELECT user_id, ROLE,mobile, state,row_number() over(partition BY user_id ORDER BY update_time DESC) rn
            FROM opay_dw_ods.ods_sqoop_base_user_di
           WHERE dt<='{pt}' ) t1
     WHERE rn = 1;
               
    create table if not exists test_db.login_{date} as 
    select dt,
           substr(from_unixtime(unix_timestamp(last_visit, 'yyyy-MM-dd HH:mm:ss')+3600),1,10) last_visit,a.user_id,role
     from 
          (select dt,user_id ,last_visit
             from opay_dw_ods.ods_sqoop_base_user_operator_df 
             where dt='{pt}' and substr(from_unixtime(unix_timestamp(last_visit, 'yyyy-MM-dd HH:mm:ss')+3600),1,10) > date_sub('{pt}',30))a 
    inner join 
          test_db.user_base_{date} b 
    on a.user_id=b.user_id;
    
       create table if not exists test_db.tran_{date} as
    select top_consume_scenario,a.user_id,dt,a.role,b.state
    from 
              ( select 
                    top_consume_scenario, originator_id user_id,dt,originator_role role
                from opay_dw.dwd_opay_transaction_record_di
                where dt>date_sub('{pt}',30) and dt<='{pt}' and create_time BETWEEN date_format(date_sub(dt, 1), 'yyyy-MM-dd 23') AND date_format(dt, 'yyyy-MM-dd 23') 
                    and originator_type = 'USER' and originator_id is not null and originator_id != ''
                group by originator_id,dt,top_consume_scenario,originator_role
                union all
                select 
                    top_consume_scenario, affiliate_id user_id,dt,affiliate_role role
                from opay_dw.dwd_opay_transaction_record_di
                where dt>date_sub('{pt}',30) and dt<='{pt}' and create_time BETWEEN date_format(date_sub(dt, 1), 'yyyy-MM-dd 23') AND date_format(dt, 'yyyy-MM-dd 23') 
                    and affiliate_type = 'USER' and affiliate_id is not null and affiliate_id != ''
                group by top_consume_scenario,affiliate_id,dt,affiliate_role
              ) a 
    inner join 
              test_db.user_base_{date} b 
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
    
      opay_active as 
           (select a.user_id,
                   a.dt 
            from 
               (select '{pt}'dt,user_id from (select user_id from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay>0) m 
                union all 
                select dt,user_id from bind_card) a
            inner join 
               (select user_id from test_db.login_{date} where last_visit<='{pt}') b 
            on a.user_id=b.user_id)
    
    INSERT overwrite TABLE opay_dw.app_opay_active_user_report_d partition (country_code,dt,target_type)
    
    SELECT 
                
                '-' city,
                ROLE,
                '-' kyc_level,
                top_consume_scenario,
                '-' register_client,
                c,
                state,
                'NG' country_code
                dt,
                target_type
            FROM (
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'bind_card_user_cnt' target_type,
                    '-' state,
                    count(DISTINCT user_id ) c
                FROM bind_card
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'bind_card_pay_user_cnt' target_type,
                    '-' state,
                    count(DISTINCT CASE WHEN pay_status='1' THEN user_id END) c
                FROM bind_card
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_d' target_type,
                    '-' state,
                    count(DISTINCT CASE WHEN last_visit='{pt}' THEN user_id END) c
                FROM test_db.login_{date}
                GROUP BY dt, ROLE
                UNION ALL 
                SELECT 
                    dt,
                    'ALL' ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_d' target_type,
                    '-' state,
                    count(DISTINCT CASE WHEN last_visit='{pt}' THEN user_id END) c
                FROM test_db.login_{date}
                GROUP BY dt
                UNION 
                ALL 
                SELECT 
                    dt,
                    ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_7d' target_type,
                    '-' state,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',7) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{date}
                GROUP BY dt, ROLE
                UNION ALL 
                SELECT 
                    dt,
                    'ALL' ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_7d' target_type,
                    '-' state,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',7) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{date}
                GROUP BY dt
                UNION ALL 
                SELECT 
                    dt,
                    ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_30d' target_type,
                    '-' state,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',30) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{date}
                GROUP BY dt, ROLE
                UNION ALL 
                SELECT 
                    dt,
                    'ALL' ROLE,
                    '-' top_consume_scenario,
                    'login_user_cnt_30d' target_type,
                    '-' state,
                    count(DISTINCT CASE WHEN last_visit>date_sub('{pt}',30) AND last_visit<='{pt}' THEN user_id END) c
                FROM test_db.login_{date}
                GROUP BY dt
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'owallet_bal_not_zero_user_cnt' target_type,
                    '-' state,
                    count(DISTINCT user_id) c
                FROM (select user_id from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet>0) m
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'owealth_bal_not_zero_user_cnt' target_type,
                    '-' state,
                    count(DISTINCT user_id) c
                FROM (select user_id from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth>0) m1
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt' target_type,
                    '-' state,
                    count(DISTINCT user_id) c
                FROM (select user_id from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay>0) m2
        
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt_7d' target_type,
                    '-' state,
                     count(DISTINCT user_id) c
                from 
                   (select user_id FROM opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay_7>0) m3
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt_30d' target_type,
                    '-' state,
                     count(DISTINCT user_id) c
                FROM 
                   (select user_id FROM opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay_30>0)m4
                UNION ALL 
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_active_user_cnt' target_type,
                    '-' state,
                    count(DISTINCT user_id) c
                FROM opay_active
                GROUP BY dt
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'opay_bal_avg' target_type,
                    '-' state,
                    sum(opay) c
                from (select user_role,opay from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay>0) m5
                group by user_role
                union all
                select 
                    '{pt}' dt,
                    'ALL' role,
                    '-' top_consume_scenario,
                    'opay_bal_avg' target_type,
                    '-' state,
                    sum(opay) c
                from (select opay from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay>0) m6
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'opay_bal_avg_30d' target_type,
                    '-' state,
                    sum(opay_30) c
                from (select user_role,opay_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay_30>0) m7
                group by user_role
                union all
                select 
                    '{pt}' dt,
                    'ALL' role,
                    '-' top_consume_scenario,
                    'opay_bal_avg_30d' target_type,
                    '-' state,
                    sum(opay_30) c
                from (select opay_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay_30>0) m8
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owealth_bal_avg' target_type,
                    '-' state,
                    sum(owealth) c
                from (select user_role,owealth from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth>0) m9
                group by user_role
                union all
                select 
                    '{pt}' dt,
                    'ALL' role,
                    '-' top_consume_scenario,
                    'owealth_bal_avg' target_type,
                    '-' state,
                    sum(owealth) c
                from (select owealth from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth>0) m10
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owealth_bal_avg_30d' target_type,
                    '-' state,
                    sum(owealth_30) c
                from (select user_role,owealth_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth_30>0) m11
                group by user_role
                union all
                select 
                    '{pt}' dt,
                    'ALL' role,
                    '-' top_consume_scenario,
                    'owealth_bal_avg_30d' target_type,
                    '-' state,
                    sum(owealth_30) c
                from (select owealth_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth_30>0) m12
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owallet_bal_avg' target_type,
                    '-' state,
                    sum(owallet) c
                from (select user_role,owallet from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet>0) m13
                group by user_role
                union all
                select 
                    '{pt}' dt,
                    'ALL' role,
                    '-' top_consume_scenario,
                    'owallet_bal_avg' target_type,
                    '-' state,
                    sum(owallet) c
                from (select owallet from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet>0) m14
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owallet_bal_avg_30d' target_type,
                    '-' state,
                    sum(owallet_30) c
                from (select user_role,owallet_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet_30>0) m15
                group by user_role
                union all
                select 
                    '{pt}' dt,
                    'ALL' role,
                    '-' top_consume_scenario,
                    'owallet_bal_avg_30d' target_type,
                    '-' state,
                    sum(owallet_30) c
                from (select owallet_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet_30>0) m16
                union all
                select 
                     '{pt}' dt,
                     role,
                     top_consume_scenario,
                     'active_user_cnt_30d' target_type,
                     state,
                     count(distinct user_id) c 
                from test_db.tran_{date}
                group by role,top_consume_scenario,state
                union all
                select 
                     '{pt}' dt,
                     role,
                     top_consume_scenario,
                     'active_user_cnt_30d' target_type,
                     'ALL' state,
                     count(distinct user_id) c 
                from test_db.tran_{date}
                group by role,top_consume_scenario
                 union all
                select 
                     '{pt}' dt,
                     role,
                     'ALL' top_consume_scenario,
                     'active_user_cnt_30d' target_type,
                     '-' state,
                     count(distinct user_id) c 
                from test_db.tran_{date}
                group by role
                 union all
                select 
                     '{pt}' dt,
                     'ALL' role,
                     top_consume_scenario,
                     'active_user_cnt_30d' target_type,
                     '-' state,
                     count(distinct user_id) c 
                from test_db.tran_{date}
                group by top_consume_scenario
                union all
                select 
                     '{pt}' dt,
                     role,
                     top_consume_scenario,
                     'active_user_cnt_7d' target_type,
                     state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt>date_sub('{pt}',7)) mm
                group by role,top_consume_scenario,state
                union all
                select 
                     '{pt}' dt,
                     role,
                     top_consume_scenario,
                     'active_user_cnt_7d' target_type,
                     'ALL' state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt>date_sub('{pt}',7)) mm
                group by role,top_consume_scenario
                union all
                select 
                     '{pt}' dt,
                     role,
                     'ALL' top_consume_scenario,
                     'active_user_cnt_7d' target_type,
                     '-' state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt>date_sub('{pt}',7)) mm
                group by role
                union all
                select 
                     '{pt}' dt,
                     'ALL' role,
                     top_consume_scenario,
                     'active_user_cnt_7d' target_type,
                     '-' state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt>date_sub('{pt}',7)) mm
                group by top_consume_scenario
                union all
                select 
                     '{pt}' dt,
                     role,
                     top_consume_scenario,
                     'active_user_cnt_d' target_type,
                     state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt='{pt}') mm
                group by role,top_consume_scenario,state
                union all
                select 
                     '{pt}' dt,
                     role,
                     top_consume_scenario,
                     'active_user_cnt_d' target_type,
                     'ALL' state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt='{pt}') mm
                group by role,top_consume_scenario
                union all
                select 
                     '{pt}' dt,
                     role,
                     'ALL' top_consume_scenario,
                     'active_user_cnt_d' target_type,
                     '-' state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt='{pt}') mm
                group by role
                union all
                select 
                     '{pt}' dt,
                     'ALL' role,
                     top_consume_scenario,
                     'active_user_cnt_d' target_type,
                     '-' state,
                     count(distinct user_id) c 
                from (select * from test_db.tran_{date} where dt='{pt}') mm
                group by top_consume_scenario
                
            ) m;
    DROP TABLE IF EXISTS test_db.user_base_{date};
    DROP TABLE IF EXISTS test_db.login_{date};
    DROP TABLE IF EXISTS test_db.tran_{date}
        



    '''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        date=ds_nodash
    )
    return HQL


def execution_data_task_id(ds, ds_nodash,  **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_active_user_report_d_sql_task(ds,ds_nodash)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opay_active_user_report_d_task = PythonOperator(
    task_id='app_opay_active_user_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_user_payment_instrument_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_user_operator_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_owealth_share_acct_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_account_user_df_prev_day_task >> app_opay_active_user_report_d_task
dwm_opay_user_balance_df_prev_day_task >> app_opay_active_user_report_d_task


