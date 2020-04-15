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
from utils.get_local_time import GetLocalTime
from plugins.CountriesAppFrame import CountriesAppFrame


args = {
    'owner': 'xiedong',
    'start_date': datetime(2020, 3, 28),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_bd_agent_trans_hf',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"
table_name = "dwm_opay_bd_agent_trans_hf"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
### 检查当前小时的分区依赖
dwd_opay_cico_record_hi_check_task = OssSensor(
    task_id='dwd_opay_cico_record_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cico_record_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_pos_transaction_record_hi_check_task = OssSensor(
    task_id='dwd_opay_pos_transaction_record_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_pos_transaction_record_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_bd_agent_change_log_hf_check_task = OssSensor(
    task_id='dwd_opay_bd_agent_change_log_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_bd_agent_change_log_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,execution_date,**op_kwargs):

    dag_ids=dag.dag_id

    #监控国家
    v_country_code='NG'

    #时间偏移量
    v_gap_hour=0

    v_date=GetLocalTime("opay",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['date']
    v_hour=GetLocalTime("opay",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['hour']

    #小时级监控
    tb_hour_task = [
        {"dag":dag,"db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code={country_code}/dt={pt}/hour={now_hour}".format(country_code=v_country_code,pt=v_date,now_hour=v_hour), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def dwm_opay_bd_agent_trans_hf_sql_task(ds, v_date):
    HQL = '''

    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    
    insert overwrite table {db}.{table} partition(country_code, dt, hour)
    select 
        coalesce(
            t3.bd_admin_user_id, 
            t2.bd_admin_user_id,  
            t1.bd_admin_user_id) as bd_admin_user_id,
        nvl(audit_suc_cnt, 0) as audit_suc_cnt, 
        nvl(audit_fail_cnt, 0) as audit_fail_cnt,
        nvl(ci_suc_order_cnt, 0) as ci_suc_order_cnt,
        nvl(ci_suc_order_amt, 0) as ci_suc_order_amt,
        nvl(co_suc_order_cnt, 0) as co_suc_order_cnt,
        nvl(co_suc_order_amt, 0) as co_suc_order_amt,
        nvl(pos_suc_amt, 0) as pos_suc_amt,
        nvl(pos_suc_cnt, 0) as pos_suc_cnt,
        date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour,
        coalesce(
            t3.country_code, 
            t2.country_code,  
            t1.country_code) as country_code,
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt,
        date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
    from (
        select 
            bd_admin_user_id, country_code,
            sum(if(to_agent_status = 1, 1, 0)) as audit_suc_cnt,
            sum(if(to_agent_status = 2, 1, 0)) as audit_fail_cnt
        from opay_dw.dwd_opay_bd_agent_change_log_hf
        where concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
            and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
            and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
            and date_format(create_time, 'yyyy-MM-dd') = '{pt}'
        group by bd_admin_user_id, country_code
    ) t1
    full join (
        select 
            bd_admin_user_id, country_code, 
            sum(if(sub_service_type='Cash In', amount, 0)) as ci_suc_order_amt,
            sum(if(sub_service_type='Cash In', 1, 0)) as ci_suc_order_cnt,
            sum(if(sub_service_type='Cash Out', amount, 0)) as co_suc_order_amt,
            sum(if(sub_service_type='Cash Out', 1, 0)) as co_suc_order_cnt
        from (
            select
                bd_admin_user_id, sub_service_type, amount, 
                date_format(create_time, 'yyyy-MM-dd') as create_date,
                country_code, 
                row_number() over(partition by order_no order by update_time desc) rn
            from opay_dw.dwd_opay_cico_record_di
            where dt = '{pt}' and order_status = 'SUCCESS' and bd_agent_status = 1 
        ) t0 where rn = 1 and create_date = '{pt}'
        group by bd_admin_user_id, country_code
    ) t2 on t1.bd_admin_user_id = t2.bd_admin_user_id and t1.country_code = t2.country_code
    full join (
        select 
            bd_admin_user_id, country_code,
            sum(amount) as pos_suc_amt,
            count(*) as pos_suc_cnt
        from (
            select
                bd_admin_user_id, amount, 
                date_format(create_time, 'yyyy-MM-dd') as create_date, 
                country_code,
                row_number() over(partition by order_no order by update_time desc) rn
            from opay_dw.dwd_opay_pos_transaction_record_hi
            where dt = '{pt}' and order_status = 'SUCCESS' and bd_agent_status = 1 
        ) t0 where rn = 1 and create_date = '{pt}'
        group by bd_admin_user_id, country_code
    ) t3 on t3.bd_admin_user_id = t2.bd_admin_user_id and t3.country_code = t2.country_code

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
            "is_hour_task": "true",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_opay_bd_agent_trans_hf_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwm_opay_bd_agent_trans_hf_task = PythonOperator(
    task_id='dwm_opay_bd_agent_trans_hf_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwd_opay_cico_record_hi_check_task >> dwm_opay_bd_agent_trans_hf_task
dwd_opay_pos_transaction_record_hi_check_task >> dwm_opay_bd_agent_trans_hf_task
dwd_opay_bd_agent_change_log_hf_check_task >> dwm_opay_bd_agent_trans_hf_task

