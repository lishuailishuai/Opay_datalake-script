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

dwm_opay_user_last_visit_df_day_task = OssSensor(
    task_id='dwm_opay_user_last_visit_df_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_user_last_visit_df/country_code=NG",
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

dwm_opay_user_first_trans_df_prev_day_task = OssSensor(
    task_id='dwm_opay_user_first_trans_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_user_first_trans_df/country_code=NG",
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

dwd_opay_user_upgrade_agent_df_day_task = OssSensor(
    task_id='dwd_opay_user_upgrade_agent_df_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_user_upgrade_agent_df/country_code=NG",
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
    create table if not exists test_db.user_base_{date} as 
    SELECT user_id, ROLE,mobile,state,register_client,kyc_level,create_time
     FROM
          (SELECT user_id, ROLE,mobile, state,nvl(register_client,'App') register_client,kyc_level,create_time,
          row_number() over(partition BY user_id ORDER BY update_time DESC) rn
            FROM opay_dw_ods.ods_sqoop_base_user_di
           WHERE dt<='{pt}' ) t1
     WHERE rn = 1;
               
    create table if not exists test_db.login_{date} as 
    select 
        m.dt,
        last_visit,
        m.user_id,
        role
    from 
        (select user_id,dt,date_format(last_visit_time,'yyyy-MM-dd') last_visit from opay_dw.dwm_opay_user_last_visit_df 
         where dt='{pt}' and date_format(last_visit_time,'yyyy-MM-dd') > date_sub('{pt}',30)) m 
    left join 
        test_db.user_base_{date} m1
    on m.user_id=m1.user_id;
    
      
    
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
                '_' country,
                '-' city,
                ROLE,
                kyc_level,
                top_consume_scenario,
                register_client,
                c,
                state,
                'NG' country_code,
                dt,
                target_type
            FROM (
                SELECT 
                    dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'bind_card_user_cnt' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
                    count(DISTINCT user_id) c
                FROM (select user_id from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet>0) m
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'owealth_bal_not_zero_user_cnt' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
                    count(DISTINCT user_id) c
                FROM (select user_id from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth>0) m1
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
                    count(DISTINCT user_id) c
                FROM (select user_id from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay>0) m2
        
                UNION ALL 
                SELECT 
                    '{pt}' dt,
                    '-' ROLE,
                    '-' top_consume_scenario,
                    'opay_bal_not_zero_user_cnt_7d' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
                    sum(opay) c
                from (select opay from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay>0) m6
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'opay_bal_avg_30d' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
                    sum(opay_30) c
                from (select opay_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and opay_30>0) m8
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owealth_bal_avg' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
                    sum(owealth) c
                from (select owealth from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth>0) m10
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owealth_bal_avg_30d' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
                    sum(owealth_30) c
                from (select owealth_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owealth_30>0) m12
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owallet_bal_avg' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
                    sum(owallet) c
                from (select owallet from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet>0) m14
                union all
                select 
                    '{pt}' dt,
                    user_role role,
                    '-' top_consume_scenario,
                    'owallet_bal_avg_30d' target_type,
                    '-' state,
                    '-' kyc_level,
                    '-' register_client,
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
                    '-' kyc_level,
                    '-' register_client,
                    sum(owallet_30) c
                from (select owallet_30 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet_30>0) m16
                union all
                select 
                    '{pt}' dt,
                     role,
                     '-' top_consume_scenario,
                     'reg_user_cnt' target_type,
                     state,
                     kyc_level,
                     register_client,
                     count(1) c 
                 from test_db.user_base_{date}
                 group by role,state,kyc_level,register_client
                 union all
                 select 
                    '{pt}' dt,
                     role,
                     '-' top_consume_scenario,
                     'new_reg_user_cnt' target_type,
                     state,
                     kyc_level,
                     register_client,
                     count(1) c 
                 from test_db.user_base_{date} where create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')
                 group by role,state,kyc_level,register_client
                 union all
                 select '{pt}' dt,
                         user_role,
                         '-' top_consume_scenario,
                         'zero_bal_acct_cnt' target_type,
                         '-' state,
                         user_level,
                         '-' register_client,
                         count(1)
                 from opay_dw.dwm_opay_user_balance_df where dt='{pt}' and owallet='0'
                 group by user_role,user_level
                 union all
                 select '{pt}' dt,
                         '-' user_role,
                         top_consume_scenario,
                         'first_pay_user_cnt' target_type,
                         '-' state,
                         '-' user_level,
                         '-' register_client,
                         count(1)
                 from opay_dw.dwm_opay_user_first_trans_df
                 WHERE dt='{pt}'
                       and date_format(trans_time,'yyyy-MM-dd')='{pt}'
                 GROUP BY top_consume_scenario
                 union all
                 select '{pt}' dt,
                         '-' user_role,
                         '-' top_consume_scenario,
                         'total_aggregators' target_type,
                         '-' state,
                         '-' user_level,
                         '-' register_client,
                         count(distinct merchant_id) c 
                 from opay_dw_ods.ods_sqoop_base_merchant_df
                 WHERE dt='{pt}' and merchant_type='aggregator'
                 union all
                 select '{pt}' dt,
                         '-' user_role,
                         '-' top_consume_scenario,
                         'new_aggregators' target_type,
                         '-' state,
                         '-' user_level,
                         '-' register_client,
                         count(distinct merchant_id) c
                 from opay_dw_ods.ods_sqoop_base_merchant_df
                 WHERE dt='{pt}' and merchant_type='aggregator' and 
                       substr(from_unixtime(unix_timestamp(create_time, 'yyyy-MM-dd HH:mm:ss')+3600),1,10)='{pt}'
                 union all
                 select '{pt}' dt,
                         '-' user_role,
                         '-' top_consume_scenario,
                         'total_subagent' target_type,
                         '-' state,
                         '-' user_level,
                         '-' register_client,
                         count(distinct user_id) c 
                 from opay_dw.dwd_opay_user_upgrade_agent_df
                 WHERE dt = if('{pt}' <= '2020-03-18', '2020-03-18', '{pt}') and is_reseller='Y'
                 union all
                 select '{pt}' dt,
                         '-' user_role,
                         '-' top_consume_scenario,
                         'new_subagent' target_type,
                         '-' state,
                         '-' user_level,
                         '-' register_client,
                         count(distinct user_id) c 
                 from opay_dw.dwd_opay_user_upgrade_agent_df
                 WHERE dt = if('{pt}' <= '2020-03-18', '2020-03-18', '{pt}') and date_format(reseller_time,'yyyy-MM-dd')='{pt}'
                 
                 
            ) m;
    DROP TABLE IF EXISTS test_db.user_base_{date};
    DROP TABLE IF EXISTS test_db.login_{date};
        



    '''.format(
        pt=ds,
        table=table_name,
        db=db_name,
        date=ds_nodash
    )
    return HQL


# 主流程
def execution_data_task_id(ds, ds_nodash, dag, **kwargs):
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
    _sql = "\n" + cf.alter_partition() + "\n" + app_opay_active_user_report_d_sql_task(ds, ds_nodash)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_opay_active_user_report_d_task = PythonOperator(
    task_id='app_opay_active_user_report_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_user_payment_instrument_df_prev_day_task >> app_opay_active_user_report_d_task
dwm_opay_user_last_visit_df_day_task >> app_opay_active_user_report_d_task
ods_sqoop_owealth_share_acct_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_account_user_df_prev_day_task >> app_opay_active_user_report_d_task
dwm_opay_user_balance_df_prev_day_task >> app_opay_active_user_report_d_task
dwm_opay_user_first_trans_df_prev_day_task >> app_opay_active_user_report_d_task
ods_sqoop_base_merchant_df_prev_day_task >> app_opay_active_user_report_d_task
dwd_opay_user_upgrade_agent_df_day_task >> app_opay_active_user_report_d_task

