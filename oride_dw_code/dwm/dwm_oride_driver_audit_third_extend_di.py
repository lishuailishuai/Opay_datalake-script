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
from airflow.sensors import OssSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'lijialong',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwm_oride_driver_audit_third_extend_di', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name="dwm_oride_driver_audit_third_extend_di"

##----------------------------------------- 依赖 ---------------------------------------## 

#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    #依赖前一天分区
    ods_sqoop_base_data_driver_extend_df_prev_day_task=UFileSensor(
        task_id='ods_sqoop_base_data_driver_extend_df_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_extend",
            pt='{{ds}}'
            ),
        bucket_name='opay-datalake',
        poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
        dag=dag
            )
    #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    ods_sqoop_base_data_driver_extend_df_prev_day_task = OssSensor(
        task_id='ods_sqoop_base_data_driver_extend_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/oride_data/data_driver_extend",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)



task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dwm_oride_driver_audit_third_extend_di_sql_task(ds):
    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select 
  id as driver_id,--司机 ID
  serv_mode,--服务模式 (0: no service, 1: in service)
  serv_status,--服务状态 (0: wait assign, 1: pick up, 2: send)
  order_rate,--接单率
  assign_order,--派单数量
  take_order,--接单数量
  avg_score,--平均评分
  total_score,--总评分
  score_times,--评分次数
  last_order_id,--最近一个订单的ID
  register_time,--注册时间
  login_time,--最后登陆时间
  is_bind ,--状态 0 未绑定 1 已绑定
  first_bind_time,--初次绑定时间
  total_pay,--总计-已打款收入
  inviter_role,--邀请者角色
  inviter_id,--邀请者ID
  block,--后台管理司机接单状态(0: 允许 1:不允许)
  serv_type  as product_id,--1 专车 2 快车
  serv_score,--司机服务分
  local_gov_ids,--行会ID,json
  updated_at,--最后更新时间
  fault ,--正常0(停运)修理1(停运)无资料2(停运)事故3(停运)扣除4(欠缴)5
  city_id,--所属城市ID
  language,--客户端语言
  end_service_time,--专车司机结束收份子钱时间
  last_online_time,--最后上线时间
  last_offline_time, --最后下线时间
  'nal' AS country_code,
             --国家码字段
  '{pt}' as dt
from oride_dw_ods.ods_sqoop_base_data_driver_extend_df
where dt='{pt}'
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
    _sql = dwm_oride_driver_audit_third_extend_di_sql_task(ds)

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


dwm_oride_driver_audit_third_extend_di_task = PythonOperator(
    task_id='dwm_oride_driver_audit_third_extend_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_data_driver_extend_df_prev_day_task>>dwm_oride_driver_audit_third_extend_di_task
