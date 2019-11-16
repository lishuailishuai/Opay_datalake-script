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
        'owner': 'yangmingze',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwd_ofood_anti_fraud_log_di', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 

##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
dwd_ofood_anti_fraud_log_di_prev_day_tesk=HivePartitionSensor(
      task_id="dwd_ofood_anti_fraud_log_di_prev_day_tesk",
      table="log_anti_ofood_oride_fraud",
      partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
      schema="oride_source",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

##----------------------------------------- 变量 ---------------------------------------## 

db_name="ofood_dw"
table_name="dwd_ofood_anti_fraud_log_di"
hdfs_path="ufile://opay-datalake/oride/ofood_dw/"+table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "ofood_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------## 


def dwd_ofood_anti_fraud_log_di_sql_task(ds):

    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE ofood_dw.{table} partition(country_code,dt)
    select 
    action,--行为
    userid,--用户id
    deviceid,--设备id
    inviterrole,--邀请人类型
    inviterid,--邀请人id
    isblacklisted,--是否在黑名单
    isvirtualdevice,--是否是虚拟设备
    driverid,--司机id
    isroot,--是否root
    isvirtual,--是否为虚拟设备
    taketime,--接单时间
    waittime,--到达接送点时间
    pickuptime,--接到乘客时间
    arrivetime,--到达终点时间
    canceltime,--订单取消时间
    cancelreason,--订单取消原因
    createtime,--创建时间
    silencefrom,--静默开始时间
    silenceto,--静默结束时间
    behaviors,--行为id
    behavior,--行为id
    silenttime,--静默时间
    waitlat,--到达接送点纬度
    waitlng,--到达接送点经度
    arrivelat,--到达终点纬度
    arrivelng,--到达终点经度
    distance1,--司机等待乘客的位置与出发地
    distance2,--到达地和目的地的距离
    userdeviceid,--用户设备id
    issilent,--是否静默
    reason,--原因
    couponid,--优惠类型
    orderid,--订单id
    abnormalstrategy, --命中策略id
    'nal' as country_code,
    dt
     from  oride_source.log_anti_ofood_oride_fraud where dt='{pt}' and action in ('OFoodUserRegister','OFoodUseCoupon','OFoodPlaceOrder','OFoodCancelOrder')
     ;
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL

#主流程
def execution_data_task_id(ds,**kargs):

    hive_hook = HiveCliHook()

    #读取sql
    _sql=dwd_ofood_anti_fraud_log_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据
    #check_key_data_task(ds)

    #生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"true","true")
    
dwd_ofood_anti_fraud_log_di_task= PythonOperator(
    task_id='dwd_ofood_anti_fraud_log_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)
 
dwd_ofood_anti_fraud_log_di_prev_day_tesk>>dwd_ofood_anti_fraud_log_di_task