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
from plugins.CountriesAppFrame import CountriesAppFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwd_oride_anti_fraud_log_di', 
    schedule_interval="10 01 * * *",
    default_args=args,
    )

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dwd_oride_anti_fraud_log_di"

##----------------------------------------- 依赖 ---------------------------------------##

#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    #依赖前一天分区
    log_anti_oride_fraud_task=HivePartitionSensor(
          task_id="log_anti_oride_fraud_task",
          table="log_anti_oride_fraud",
          partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
          schema="oride_source",
          poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
          dag=dag
        )
    # 路径
    hdfs_path="ufile://opay-datalake/oride/oride_dw/dwd_oride_anti_fraud_log_new_di"
else:
    print("成功")

    # 依赖前一天分区
    log_anti_oride_fraud_task = HivePartitionSensor(
        task_id="log_anti_oride_fraud_task",
        table="log_anti_oride_fraud",
        partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
        schema="oride_source",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 


def dwd_oride_anti_fraud_log_di_sql_task(ds):

    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    
    select action,--行为
        behavior_id,--行为id
        user_id,--用户id
        driver_id,--司机id
        order_id,--订单id
        extend,--扩展信息json
        (`timestamp`+1*60*60) as `timestamp`,--时间戳
        'nal' as country_code,
        dt
    from  oride_source.log_anti_oride_fraud 
    where dt='{pt}' 
    and action in ('UserRegister','UserLogin','DriverLogin',
        'SilenceUser','UserCancelOrder','DriverCancelOrder','OrderWait','SilenceDriver',
        'OrderArrive','SilenceDriver2','SilenceDriver3','OrderFinish','IsUserSilent','IsDriverSilent')
    ;

'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
        )
    return HQL

#主流程
def execution_data_task_id(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_oride_anti_fraud_log_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()
    
dwd_oride_anti_fraud_log_di_task= PythonOperator(
    task_id='dwd_oride_anti_fraud_log_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

log_anti_oride_fraud_task>>dwd_oride_anti_fraud_log_di_task