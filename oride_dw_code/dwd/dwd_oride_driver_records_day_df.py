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
    'owner': 'lijialong',
    'start_date': datetime(2019, 10, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_driver_records_day_df',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
ods_sqoop_base_data_driver_records_day_df_task = UFileSensor(
    task_id='ods_sqoop_base_data_driver_records_day_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_records_day",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_driver_records_day_df"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_driver_records_day_df_task = HiveOperator(
    task_id='dwd_oride_driver_records_day_df_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt)

        SELECT
            id,--'ID'
            agenter_id,--'小老板ID'
            driver_id,--'司机ID'
            day,--'天'
            count_orders_assign,--'当日派单数'
            count_orders_all,--'当日接单数'
            count_orders_finish,--'当日完成单数'
            count_orders_complaint,--'当日被投诉单数'
            count_orders_reward,--'当日获取奖励金单数'
            amount_all,--'当日总收入'
            amount_pay_online,--'当日总收入-线上支付金额'
            amount_pay_offline,--'当日总收入-线下支付金额'
            amount_reward,--'当日总收入-奖励金'
            amount_reward_json,--'当日总收入-奖励金-明细,json格式'
            amount_reward_type_invite,--'当日总收入-奖励金-邀请奖励'
            amount_reward_type_1,--'当日总收入-奖励金-新人奖励'
            amount_reward_type_0,--'当日总收入-奖励金-满单奖励'
            is_finish_service,--'当日收入是否完成份子数. 0没有完成，1完成'
            amount_service,--'当日骑手份子钱-总数'
            amount_platform,--'当日骑手份子钱-平台抽成80%'
            amount_agenter,--'当日骑手份子钱-小老板抽成20%'
            amount_true,--'当日骑手-实际到手收入'
            amount_recharge,--'当日骑手-补录份子钱(充值)'
            passenger_orders,--'当日乘客使用优惠券次数'
            passenger_amount,--'当日乘客使用优惠券金额'
            work_hours,--'当日工作时长,按小时计算'
            payment_status,--'状态：0未打款 1已打款'
            balance_status,--'结算状态：0未结算 1已结算'
            created_at,--'申请时间'
            updated_at,--'更新时间'
            amount_tip,--'当日-小费收入'
            amount_tip_offline,--'当日-小费收入(线下支付)
            'nal' as country_code,
            '{pt}' as dt
        FROM
            oride_dw_ods.ods_sqoop_base_data_driver_records_day_df
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

ods_sqoop_base_data_driver_records_day_df_task >> sleep_time >> dwd_oride_driver_records_day_df_task >> touchz_data_success
