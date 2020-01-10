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
    'start_date': datetime(2019, 12, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opay_user_report_sum_d',
                  schedule_interval="00 03 * * *",
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

dwd_opay_account_balance_df_prev_day_task = OssSensor(
    task_id='dwd_opay_account_balance_df_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_account_balance_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwm_opay_user_first_tran_di_prev_day_task = OssSensor(
    task_id='dwm_opay_user_first_tran_di_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwm_opay_user_first_tran_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag":dag, "db": "opay_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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
db_name = "opay_dw"

table_name = "app_opay_user_report_sum_d"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name


def app_opay_user_report_sum_d_sql_task(ds):
    HQL = '''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    --总注册
    WITH user_reg AS
  (SELECT register_client,
          ROLE,
          kyc_level,
          '-' top_consume_scenario,
          count(1) reg_user_cnt,
          NULL AS new_reg_user_cnt,
          NULL AS zero_bal_acct_cnt,
          NULL AS first_pay_user_cnt
        
   FROM
     (SELECT user_id,
             ROLE,
             mobile,
             register_client,
             kyc_level,
             dt,
             row_number() over(partition BY user_id
                               ORDER BY update_time DESC) rn
      FROM opay_dw.dim_opay_user_base_di
      WHERE dt<='{pt}' ) t1
   WHERE rn = 1
   GROUP BY register_client,
            ROLE,
            kyc_level),
--当天新增
    user_reg_curr as 
   (SELECT nvl(register_client,'App') register_client,
          ROLE,
          kyc_level,
          '-' top_consume_scenario,
          NULL AS reg_user_cnt,
          count(1) new_reg_user_cnt,
          NULL AS zero_bal_acct_cnt,
          NULL AS first_pay_user_cnt
    from opay_dw.dim_opay_user_base_di 
    where dt='{pt}' and create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')
    GROUP BY nvl(register_client,'App'),
            ROLE,
            kyc_level
   ),
--用户余额信息
     balance AS
  (SELECT '-' register_client,
              user_role,
              user_level,
              '-' top_consume_scenario,
                  NULL AS reg_user_cnt,
                  NULL AS new_reg_user_cnt,
                  count(1) zero_bal_acct_cnt,
                  NULL AS first_pay_user_cnt
   FROM opay_dw.dwd_opay_account_balance_df
   WHERE dt='{pt}'
     AND user_type='USER'
     AND account_type='CASHACCOUNT'
     AND balance='0'
   GROUP BY user_role,
            user_level),
--首购表
     first_tran AS
  (SELECT '-' register_client,
              '-' ROLE,
                  '-' kyc_level,
                      top_consume_scenario,
                      NULL AS reg_user_cnt,
                      NULL AS new_reg_user_cnt,
                      NULL AS zero_bal_acct_cnt,
                      count(1) first_pay_user_cnt
   FROM opay_dw.dwm_opay_user_first_tran_di
   WHERE dt='{pt}'
     AND originator_type='USER'
   GROUP BY top_consume_scenario)

INSERT overwrite TABLE opay_dw.app_opay_user_report_sum_d partition (dt='{pt}')
select register_client,ROLE,kyc_level,top_consume_scenario,
       sum(nvl(reg_user_cnt,0)) reg_user_cnt,
       sum(nvl(new_reg_user_cnt,0)) new_reg_user_cnt,
       sum(nvl(zero_bal_acct_cnt,0)) zero_bal_acct_cnt,
       sum(nvl(first_pay_user_cnt,0)) first_pay_user_cnt
from 
   (SELECT * FROM user_reg
   UNION ALL
   SELECT * FROM user_reg_curr
   UNION ALL
   SELECT * FROM balance
   UNION ALL
   SELECT * FROM first_tran) m 
group by register_client,ROLE,kyc_level,top_consume_scenario

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opay_user_report_sum_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")


app_opay_user_report_sum_d_task = PythonOperator(
    task_id='app_opay_user_report_sum_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opay_user_base_di_prev_day_task >> app_opay_user_report_sum_d_task
dwd_opay_account_balance_df_prev_day_task >> app_opay_user_report_sum_d_task
dwm_opay_user_first_tran_di_prev_day_task >> app_opay_user_report_sum_d_task


