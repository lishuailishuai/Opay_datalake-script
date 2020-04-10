import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook
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
from airflow.sensors import OssSensor
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

dag = airflow.DAG('dim_opay_bd_relation_df',
                  schedule_interval="10 01 * * *",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##

dim_opay_bd_admin_user_df_prev_day_task = OssSensor(
    task_id='dim_opay_bd_admin_user_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_bd_admin_user_df/country_code=NG",
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
table_name="dim_opay_bd_relation_df"
hdfs_path="oss://opay-datalake/opay/opay_dw/"+table_name

##---- hive operator ---##
def dim_opay_bd_relation_df_sql_task(ds):
    HQL='''
    
    set mapred.max.split.size=1000000;
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    with bd_data as (
        select 
            bd_admin_user_id, bd_admin_user_name, if(bd_admin_leader_id = 0, bd_admin_user_id, bd_admin_leader_id) bd_admin_leader_id,
            bd_admin_job_id, create_time, update_time, status
        from opay_dw.dim_opay_bd_admin_user_df 
        where dt = '{pt}' and bd_admin_job_id > 0
    )  
    insert overwrite table {db}.{table} partition (country_code = 'NG', dt = '{pt}')
    select 
          t6.bd_admin_user_id,
          t6.bd_admin_user_name,
          t6.bd_admin_job_id,
          t6.status,
          -- 第六季
          case
            when t6.bd_admin_job_id = 6 then t6.bd_admin_user_id 
            else null
            end as job_bd_user_id,
          -- 第五季
          case
            when t5.bd_admin_job_id = 5 then t5.bd_admin_user_id 
            when t6.bd_admin_job_id = 5 then t6.bd_admin_user_id
            else null
            end as job_bdm_user_id,    
          -- 第四季
          case
            when t4.bd_admin_job_id = 4 then t4.bd_admin_user_id
            when t6.bd_admin_job_id = 4 then t6.bd_admin_user_id
            when t5.bd_admin_job_id = 4 then t5.bd_admin_user_id
            
            else null
            end as job_rm_user_id,    
          -- 第三季
          case
            when t3.bd_admin_job_id = 3 then t3.bd_admin_user_id
            when t4.bd_admin_job_id = 3 then t4.bd_admin_user_id
            when t5.bd_admin_job_id = 3 then t5.bd_admin_user_id
            when t6.bd_admin_job_id = 3 then t6.bd_admin_user_id
            else null
            end as job_cm_user_id,   
         -- 第二季
          case
            when t2.bd_admin_job_id = 2 then t2.bd_admin_user_id
            when t3.bd_admin_job_id = 2 then t3.bd_admin_user_id
            when t4.bd_admin_job_id = 2 then t4.bd_admin_user_id
            when t5.bd_admin_job_id = 2 then t5.bd_admin_user_id
            when t6.bd_admin_job_id = 2 then t6.bd_admin_user_id
            else null
            end as job_hcm_user_id,    
        -- 第一季
          case
            when t1.bd_admin_job_id = 1 then t1.bd_admin_user_id
            when t2.bd_admin_job_id = 1 then t2.bd_admin_user_id
            when t3.bd_admin_job_id = 1 then t3.bd_admin_user_id
            when t4.bd_admin_job_id = 1 then t4.bd_admin_user_id
            when t5.bd_admin_job_id = 1 then t5.bd_admin_user_id
            when t6.bd_admin_job_id = 1 then t6.bd_admin_user_id
            else null
            end as job_pic_user_id,
            t6.create_time,
            t6.update_time
      from bd_data t6
      left join bd_data t5 on t6.bd_admin_leader_id = t5.bd_admin_user_id
      left join bd_data t4 on t5.bd_admin_leader_id = t4.bd_admin_user_id
      left join bd_data t3 on t4.bd_admin_leader_id = t3.bd_admin_user_id
      left join bd_data t2 on t3.bd_admin_leader_id = t2.bd_admin_user_id
      left join bd_data t1 on t2.bd_admin_leader_id = t1.bd_admin_user_id
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
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
            "is_country_partition": "false",
            "is_result_force_exist": "false",
            "execute_time": v_execution_time,
            "is_hour_task": "false",
            "frame_type": "local",
            "business_key": "opay"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dim_opay_bd_relation_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dim_opay_bd_relation_df_task = PythonOperator(
    task_id='dim_opay_bd_relation_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_time': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dim_opay_bd_admin_user_df_prev_day_task >> dim_opay_bd_relation_df_task