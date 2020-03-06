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

dag = airflow.DAG('app_opay_active_user_report_w',
                  schedule_interval="00 03 * * *",
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

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag, "db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "dt={pt}".format(pt=ds), "timeout": "3000"}
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

table_name = "app_opay_active_user_report_w"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_active_user_report_w_sql_task(ds,ds_nodash):
    HQL = '''

    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
      DROP TABLE IF EXISTS test_db.user_base_w_{date};
    DROP TABLE IF EXISTS test_db.login_w_{date};
    
    create table test_db.user_base_w_{date} as 
     SELECT user_id,
          ROLE,
         mobile
     FROM
        (SELECT user_id,
                ROLE,
                mobile,
             row_number() over(partition BY user_id
                               ORDER BY update_time DESC) rn
      FROM opay_dw_ods.ods_sqoop_base_user_di
      WHERE dt<='{pt}' ) t1
   WHERE rn = 1;
  create table test_db.login_w_{date} as 
SELECT a.dt,
          a.user_id,
          ROLE,
          last_visit
   FROM
     (SELECT dt,
             user_id,
             substr(from_unixtime(unix_timestamp(last_visit, 'yyyy-MM-dd HH:mm:ss')+3600),1,10) last_visit
      FROM opay_dw_ods.ods_sqoop_base_user_operator_df
      WHERE dt='{pt}') a
INNER JOIN test_db.user_base_w_{date} b ON a.user_id=b.user_id;

INSERT overwrite TABLE opay_dw.app_opay_active_user_report_w partition (dt,target_type)
SELECT '-' country_code,
           '-' city,
               ROLE,
               '-' kyc_level,
                   '-' top_consume_scenario,
                   '-' register_client,
                       c,
                       dt,
                       target_type
FROM (
SELECT dt,
       ROLE,
       'login_user_cnt_w' target_type,
                           count(DISTINCT user_id) c
FROM test_db.login_w_{date}
WHERE last_visit>=date_sub(next_day('{pt}', 'mo'), 7)
  AND last_visit<='{pt}'
GROUP BY dt,
         ROLE
union all
SELECT dt,
      'ALL' ROLE,
       'login_user_cnt_w' target_type,
                           count(DISTINCT user_id) c
FROM test_db.login_w_{date}
WHERE last_visit>=date_sub(next_day('{pt}', 'mo'), 7)
  AND last_visit<='{pt}'
GROUP BY dt
)m;

  DROP TABLE IF EXISTS test_db.user_base_w_{date};
    DROP TABLE IF EXISTS test_db.login_w_{date};
    




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
    _sql = app_opay_active_user_report_w_sql_task(ds,ds_nodash)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_active_user_report_w_task = PythonOperator(
    task_id='app_opay_active_user_report_w_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_user_di_prev_day_task >> app_opay_active_user_report_w_task
ods_sqoop_base_user_operator_df_prev_day_task >> app_opay_active_user_report_w_task


