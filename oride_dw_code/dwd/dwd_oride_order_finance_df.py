# -*- coding: utf-8 -*-
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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'chenlili',
    'start_date': datetime(2019, 9, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_order_finance_df',
                  schedule_interval="40 01 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_df_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_driver_recharge_records_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_recharge_records_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_recharge_records",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_driver_reward_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_reward_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_reward",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
ods_sqoop_base_data_driver_records_day_df_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_records_day_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_records_day",
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
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_order_finance_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##
def dwd_oride_order_finance_df_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=TRUE;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    
    
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    SELECT ord.city_id,
    ord.product_id,
     ord.order_id, --订单号
     ord.create_date,--订单日期
     ord.driver_id, --司机id
     sum(nvl(if(recharge.amount_reason in(4,5,7),recharge.amount,0),0.0)) AS recharge_amount, --资金调整金额
     sum(nvl(reward.amount,0.0)) AS reward_amount, --奖励金额
     sum(nvl(records.amount_pay_online,0.0)) AS amount_pay_online, --当日总收入-线上支付金额
     sum(nvl(records.amount_pay_offline,0.0)) AS amount_pay_offline, --当日总收入-线下支付金额
     ord.driver_serv_type, --订单表中司机业务类型字段
     sum(nvl(if(recharge.amount_reason=6,abs(recharge.amount),0),0.0)) AS phone_amount, --手机还款
     sum(nvl(records.amount_all,0.0)) AS amount_all, --当日总收入
     sum(nvl(abs(records.amount_agenter),0.0)) AS amount_agenter, --司机份子钱
     'nal' as country_code,
     '{pt}' as dt
    FROM
      (SELECT *
       FROM oride_dw.dwd_oride_order_base_include_test_df
       WHERE dt IN('{pt}',
                   'his')) ord
    LEFT JOIN
      (SELECT *
       FROM oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df
       WHERE dt='{pt}'
         AND amount_reason in(4,5,6,7)) recharge ON ord.order_id=recharge.order_id
    LEFT JOIN
      (SELECT *
       FROM oride_dw_ods.ods_sqoop_base_data_driver_reward_df
       WHERE dt='{pt}') reward ON ord.order_id=reward.order_id
    LEFT JOIN
      (SELECT driver_id,
              from_unixtime(DAY,'yyyy-MM-dd') AS DAY,
              amount_pay_online,  --线上支付金额
              amount_pay_offline,   --线下支付金额
              amount_all,  --司机总收入
              amount_agenter --司机份子钱
       FROM oride_dw_ods.ods_sqoop_base_data_driver_records_day_df
       WHERE dt='{pt}') records ON ord.driver_id=records.driver_id
    AND substr(ord.finish_time,1,10)=records.day
    where ord.city_id<>999001
    and ord.driver_id<>1
    GROUP BY ord.city_id,
    ord.product_id,
    ord.order_id, --订单号
     ord.create_date,--订单日期
     ord.driver_id,
     ord.driver_serv_type;
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_oride_order_finance_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_order_finance_df_task = PythonOperator(
    task_id='dwd_oride_order_finance_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_order_base_include_test_df_prev_day_task >> \
ods_sqoop_base_data_driver_recharge_records_df_prev_day_task >> \
ods_sqoop_base_data_driver_reward_df_prev_day_task >>\
ods_sqoop_base_data_driver_records_day_df_prev_day_task >>\
dwd_oride_order_finance_df_task
