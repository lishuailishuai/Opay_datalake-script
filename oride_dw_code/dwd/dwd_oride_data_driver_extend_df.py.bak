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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 10, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_data_driver_extend_df',
                  schedule_interval="20 01 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
ods_sqoop_base_data_driver_extend_df_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_extend_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_extend",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
) 

##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_data_driver_extend_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_data_driver_extend_df_task = HiveOperator(
    task_id='dwd_oride_data_driver_extend_df_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt)

        SELECT
            id,--司机 ID, 
            serv_mode,--服务模式 (0: no service, 1: in service, 99:招手停), 
            serv_status,--服务状态 (0: wait assign, 1: pick up, 2:wait, 3: send, 4:arrive, 5:pick order), 
            order_rate,--接单率, 
            assign_order,--派单数量, 
            take_order,--接单数量, 
            avg_score,--平均评分, 
            total_score,--总评分, 
            score_times,--评分次数, 
            last_order_id,--最近一个订单的ID, 
            register_time,--注册时间, 
            login_time,--最后登陆时间, 
            is_bind,--状态 0 未绑定 1 已绑定, 
            first_bind_time,--初次绑定时间, 
            total_pay,--总计-已打款收入, 
            inviter_role,--, 
            inviter_id,--, 
            block,--后台管理司机接单状态(0: 允许 1:不允许), 
            serv_type,--1 专车 2 快车, 
            serv_score,--司机服务分, 
            local_gov_ids,--行会ID,json, 
            updated_at,--最后更新时间, 
            fault,--正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5, 
            city_id,--所属城市ID, 
            language,--客户端语言, 
            end_service_time,--专车司机结束收份子钱时间, 
            last_online_time,--最后上线时间, 
            last_offline_time,--最后下线时间, 
            avoid_highway,--避开高速 (0: 禁用 1: 启用), 
            rongcloud_token,--融云token, 
            support_carpool,--是否支持拼车, 
            last_trip_id,--最近一个行程的ID, 
            assign_mode,--强派模式 (0: 禁用 1: 启用), 
            auto_start,--自动开始 (0: 禁用 1: 启用)
            'nal' as country_code,
            '{pt}' as dt
        FROM
            oride_dw_ods.ods_sqoop_base_data_driver_extend_df
        WHERE
            dt='{pt}'
        ;
'''.format(
        pt='{{ds}}',
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name
    ),
    dag=dag
)

#生成_SUCCESS
def check_success(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"table":"{dag_name}".format(dag_name=dag_ids),"hdfs_path": "{hdfsPath}/country_code=nal/dt={pt}".format(pt=ds,hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)

touchz_data_success= PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_data_driver_extend_df_task >> sleep_time >> dwd_oride_data_driver_extend_df_task >> touchz_data_success
