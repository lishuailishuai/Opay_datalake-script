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
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

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
                  schedule_interval="40 00 * * *",
                  default_args=args,
                  catchup=False)


##----------------------------------------- 变量 ---------------------------------------##
db_name = "oride_dw"
table_name = "dwd_oride_driver_records_day_df"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

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
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:
    print("成功")
    ods_sqoop_base_data_driver_records_day_df_task = OssSensor(
        task_id='ods_sqoop_base_data_driver_records_day_df_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_records_day",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)



task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_driver_records_day_df_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt)

        SELECT
            id,--'ID'
            agenter_id,--'小老板ID'
            driver_id,--'司机ID'
            if(day=0,0,(day+1*60*60)) as day,--'天'
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
            if(created_at=0,0,(created_at+1*60*60)) as created_at,--'申请时间'
            if(updated_at=0,0,(updated_at+1*60*60)) as updated_at,--'更新时间'
            amount_tip,--'当日-小费收入'
            amount_tip_offline,--'当日-小费收入(线下支付)
            amount_additional, -- 附加费-线上
            amount_additional_offline,  --附加费-线下
            'nal' as country_code,
            '{pt}' as dt
        FROM
            oride_dw_ods.ods_sqoop_base_data_driver_records_day_df
        WHERE
            dt='{pt}'
        ;
'''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_oride_driver_records_day_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    #check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_oride_driver_records_day_df_task = PythonOperator(
    task_id='dwd_oride_driver_records_day_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)


ods_sqoop_base_data_driver_records_day_df_task >> dwd_oride_driver_records_day_df_task
