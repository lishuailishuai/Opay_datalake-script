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
    'start_date': datetime(2020, 4, 7),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('ods_sqoop_base_user_operator_df',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

ods_binlog_user_operator_hi_check_task = OssSensor(
    task_id='ods_binlog_user_operator_hi_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=22/_SUCCESS'.format(
        hdfs_path_str="opay_binlog/opay_user_db.opay_user.user_operator",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_user_operator_df_pre_check_task = OssSensor(
    task_id='ods_sqoop_user_operator_df_pre_check_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop/opay_user/user_operator",
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
        {"dag": dag, "db": "opay_dw_ods", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name = "opay_dw_ods"

table_name = "ods_sqoop_base_user_operator_df"
hdfs_path = "oss://opay-datalake/opay_dw_sqoop/opay_user/user_operator"
config = eval(Variable.get("opay_time_zone_config"))


def ods_sqoop_base_user_operator_df_sql_task(ds):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    with 
        user_operator_di as (
            SELECT 
                id,
                user_id,
                mobile,
                password,
                salt,
                unblock_time,
                last_visit,
                from_unixtime(cast(cast(create_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as create_time,
                from_unixtime(cast(cast(update_time as bigint)/1000 as bigint),'yyyy-MM-dd HH:mm:ss') as update_time
        
            from (
                select 
                    *,
                    row_number() over(partition by user_id order by `__ts_ms` desc,`__file` desc,cast(`__pos` as int) desc) rn
                FROM opay_dw_ods.ods_binlog_base_user_operator_hi
                where concat(dt,' ',hour) between '{pt_y} 23' and '{pt} 22' and `__deleted` = 'false'
            ) m 
            where rn=1
        )
    
    insert overwrite table {db}.{table} partition (dt)
    select 
        id,
        user_id,
        mobile,
        password,
        salt,
        unblock_time,
        last_visit,
        create_time,
        update_time,

        '{pt}'
    from (
        select 
            id,
            user_id,
            mobile,
            password,
            salt,
            unblock_time,
            last_visit,
            create_time,
            update_time,

            row_number()over(partition by user_id order by update_time desc) rn 
        from (
            select 
                id,
                user_id,
                mobile,
                password,
                salt,
                unblock_time,
                last_visit,
                create_time,
                update_time
            from {db}.{table} 
            where dt='{pt_y}' 
            union all
            select 
                * 
            from user_operator_di
        )m
    )m1 where rn=1
    
     

    '''.format(
        pt=ds,
        pt_y=airflow.macros.ds_add(ds, -1),
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
            "is_countries_online": "false",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "false",
            "is_result_force_exist": "true",
            "execute_time": v_execution_time,
            "is_hour_task": "false",
            "frame_type": "utc",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + ods_sqoop_base_user_operator_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


ods_sqoop_base_user_operator_df_task = PythonOperator(
    task_id='ods_sqoop_base_user_operator_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

ods_binlog_user_operator_hi_check_task >> ods_sqoop_base_user_operator_df_task
ods_sqoop_user_operator_df_pre_check_task >> ods_sqoop_base_user_operator_df_task


