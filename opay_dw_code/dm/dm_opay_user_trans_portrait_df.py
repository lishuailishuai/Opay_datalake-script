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
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 22),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_opay_user_trans_portrait_df',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opay_user_transaction_record_df_prev_day_task = OssSensor(
    task_id='dwd_opay_user_transaction_record_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_user_transaction_record_df/country_code=NG",
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

table_name = "dm_opay_user_trans_portrait_df"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def dm_opay_user_trans_portrait_df_sql_task(ds):
    HQL = '''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    
    insert overwrite table {db}.{table} partition(country_code, dt)
    select 
        t1.user_id, t1.gender, t1.user_role, t1.user_kyc_level, 
        t1.user_state, t1.user_city, t1.lga, t1.address, 
        t1.mobile, t1.register_time,
    
        t2.upgrage_agent_time, t2.is_reseller, t2.reseller_time, t2.aggregator_id,
    
        t3.last_login_time, 
    
        -- t4.bind_card_cnt, t4.bind_card_suc_cnt,
        0 as bind_card_cnt, 0 as bind_card_suc_cnt,
    
        t5.first_trans_time, t5.first_trans_service_type, 
        t5.first_trans_originator_or_affiliate, t5.first_trans_amount,
        if(t5.first_trans_time is null, null, datediff(t5.first_trans_time, t1.register_time)) as first_trans_reg_diff,
    
        t6.agent_first_trans_time, t6.agent_first_trans_service_type, 
        t6.agent_first_trans_originator_or_affiliate, t6.agent_first_trans_amount, 
        if(t6.agent_first_trans_time is null or t5.first_trans_time is null, null, datediff(t6.agent_first_trans_time, t5.first_trans_time)) as agent_first_trans_time,
        if(t6.agent_first_trans_time is null or t2.upgrage_agent_time is null, null, datediff(t6.agent_first_trans_time, t2.upgrage_agent_time)) as agent_first_trans_upgrage_diff,
    
        t7.last_trans_time, t7.last_trans_service_type, t7.last_trans_originator_or_affiliate, 
        if(t5.first_trans_time is null or t7.last_trans_time is null, null, datediff(t7.last_trans_time, t5.first_trans_time)) as last_trans_to_first_trans_diff,
        if(t6.agent_first_trans_time is null or t7.last_trans_time is null, null, datediff(t7.last_trans_time, t6.agent_first_trans_time)) as last_trans_to_agent_first_trans_diff,
        if(t7.last_trans_time is null, null, datediff('{pt}', t7.last_trans_time)) as last_trans_to_current_diff, 
    
        order_cnt_d, order_suc_cnt_d, order_suc_amt_d, 
        order_suc_cnt_w, order_suc_amt_w,
        order_suc_cnt_m, order_suc_amt_m, 
        order_suc_cnt_y, order_suc_amt_y,
        order_suc_cnt_7d, order_suc_amt_7d,
        order_suc_cnt_30d, order_suc_amt_30d,
    
        owallet_bal, 
        owallet_bal_avg_w, 
        owallet_bal_avg_m, 
        owallet_bal_avg_7d, 
        owallet_bal_avg_30d, 
        owealth_bal, 
        owealth_bal_avg_w, 
        owealth_bal_avg_m, 
        owealth_bal_avg_7d, 
        owealth_bal_avg_30d
    
    
    from (
        select 
            user_id, gender, role as user_role, kyc_level as user_kyc_level, state as user_state, create_time as register_time,
            mobile, city as user_city, lga, address
        from opay_dw.dim_opay_user_base_df 
        where dt = '{pt}'
    ) t1 
    left join (
        select
            user_id, upgrade_time as upgrage_agent_time, is_reseller, reseller_time, aggregator_id
        from opay_dw.dwd_opay_user_upgrade_agent_df 
        where dt = '{pt}'
    ) t2 on t1.user_id = t2.user_id
    left join (
        select 
            user_id, last_visit as last_login_time
        from opay_dw.dwm_opay_user_last_visit_df 
        where dt = '{pt}'
    ) t3 on t1.user_id = t3.user_id
    -- left join (
    -- 	select 
    -- 		user_id
    -- 	from 
    -- ) t4 on t1.user_id = t4.user_id
    left join (
        select 
            user_id, trans_time as first_trans_time, sub_service_type as first_trans_service_type, 
            originator_or_affiliate as first_trans_originator_or_affiliate,
            amount as first_trans_amount
        from opay_dw.dwm_opay_user_first_trans_df
        where dt = '{pt}'
    ) t5 on t1.user_id = t5.first_trans_amount
    left join (
        select 
            user_id, trans_time as agent_first_trans_time, sub_service_type as agent_first_trans_service_type, 
            originator_or_affiliate as agent_first_trans_originator_or_affiliate,
            amount as agent_first_trans_amount
        from opay_dw.dwm_opay_agent_first_trans_df
        where dt = '{pt}'
    ) t6 on t1.user_id = t6.user_id
    left join (
        select
            user_id, trans_time as last_trans_time, sub_service_type as last_trans_service_type,
            originator_or_affiliate as last_trans_originator_or_affiliate
        from opay_dw.dwm_opay_user_last_trans_df
        where dt = '{pt}'
    ) t7 on t1.user_id = t7.user_id
    left join (
        select
            user_id,
            order_cnt_d, order_suc_cnt_d, order_suc_amt_d, 
            order_suc_cnt_w, order_suc_amt_w,
            order_suc_cnt_m, order_suc_amt_m, 
            order_suc_cnt_y, order_suc_amt_y,
            order_suc_cnt_7d, order_suc_amt_7d,
            order_suc_cnt_30d, order_suc_amt_30d
        from opay_dw.dwm_opay_user_trans_aggr_df
        where dt = if('{pt}' <= '2020-03-22', '2020-03-22', '{pt}') 
    ) t8 on t1.user_id = t8.user_id
    left join (
        select
            user_id, 
            owallet as owallet_bal, 
            owallet_w as owallet_bal_avg_w, 
            owallet_m as owallet_bal_avg_m, 
            owallet_7 as owallet_bal_avg_7d, 
            owallet_30 as owallet_bal_avg_30d, 
            owealth as owealth_bal, 
            owealth_w as owealth_bal_avg_w, 
            owealth_m as owealth_bal_avg_m, 
            owealth_7 as owealth_bal_avg_7d, 
            owealth_30 as owealth_bal_avg_30d
        from opay_dw.dwm_opay_user_balance_df
        where dt = '{pt}'
    ) t9 on t1.user_id = t9.user_id
    
    

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dm_opay_user_trans_portrait_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dm_opay_user_trans_portrait_df_task = PythonOperator(
    task_id='dm_opay_user_trans_portrait_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_user_transaction_record_df_prev_day_task >> dm_opay_user_trans_portrait_df_task

