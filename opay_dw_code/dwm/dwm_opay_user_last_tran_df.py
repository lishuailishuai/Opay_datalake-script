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
    'start_date': datetime(2020, 2, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_opay_user_last_tran_df',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opay_transaction_record_di_prev_day_task = OssSensor(
    task_id='dwd_opay_transaction_record_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transaction_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dwm_opay_user_last_tran_df_prev_day_task = OssSensor(
    task_id='dwm_opay_user_last_tran_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_user_last_tran_df/country_code=NG",
        pt='{{macros.ds_add(ds, -1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw"

table_name = "dwm_opay_user_last_tran_df"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def dwm_opay_user_last_tran_df_sql_task(ds):
    HQL = '''
    set  hive.exec.max.dynamic.partitions.pernode=1000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with tran as 
     (select order_no,
           create_time,
           amount,
           top_service_type,
           sub_service_type,
           originator_id user_id,
           'originator' flag,
           originator_type user_type,
           originator_role user_role,
           originator_kyc_level user_kyc_level,
           top_consume_scenario,
           sub_consume_scenario,
           client_source,
           country_code,
           dt
      from opay_dw.dwd_opay_transaction_record_di a where dt='{pt}' and originator_id is not null and originator_id<>''
        and create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')
     union all
     select order_no,
           create_time,
           amount,
           top_service_type,
           sub_service_type,
           affiliate_id user_id,
           'affiliate' flag,
           affiliate_type user_type,
           affiliate_role user_role,
           '-' user_kyc_level,
           top_consume_scenario,
           sub_consume_scenario,
           client_source,
           country_code,
           dt
      from opay_dw.dwd_opay_transaction_record_di a where dt='{pt}' and affiliate_id is not null and affiliate_id<>''
        and create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')
     )
     INSERT overwrite TABLE opay_dw.dwm_opay_user_last_tran_df partition (country_code,dt)
     select COALESCE(a.order_no,b.order_no) order_no,
            COALESCE(a.create_time,b.tran_time) create_time,
            COALESCE(a.amount,b.amount) amount,
            COALESCE(a.top_service_type,b.top_service_type) top_service_type,
            COALESCE(a.sub_service_type,b.sub_service_type) sub_service_type,
            COALESCE(a.user_id,b.user_id) user_id,
            COALESCE(a.flag,b.flag) flag,
            COALESCE(a.user_type,b.user_type) user_type,
            COALESCE(a.user_role,b.user_role) user_role,
            COALESCE(a.user_kyc_level,b.user_kyc_level) user_kyc_level,
            COALESCE(a.top_consume_scenario,b.top_consume_scenario) top_consume_scenario,
            COALESCE(a.sub_consume_scenario,b.sub_consume_scenario) sub_consume_scenario,
            COALESCE(a.client_source,b.client_source) client_source,
            COALESCE(a.dt,b.last_dt) last_dt,
            COALESCE(a.country_code,b.country_code) country_code,
            '{pt}' dt
     from
       (select m.* 
        from 
           (select a.*,row_number()over(partition by user_id,flag order by create_time desc) rn 
             from tran a) m 
        where rn=1
       ) a 
     full join 
       (select * from opay_dw.dwm_opay_user_last_tran_df where dt='{yesterday}') b 
     on a.user_id=b.user_id and a.flag=b.flag


    '''.format(
        pt=ds,
        yesterday=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_opay_user_last_tran_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_opay_user_last_tran_df_task = PythonOperator(
    task_id='dwm_opay_user_last_tran_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_transaction_record_di_prev_day_task >> dwm_opay_user_last_tran_df_task
dwm_opay_user_last_tran_df_prev_day_task >> dwm_opay_user_last_tran_df_task


