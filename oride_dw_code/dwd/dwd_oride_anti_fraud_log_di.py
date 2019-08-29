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
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwd_oride_anti_fraud_log_di', 
    schedule_interval="00 03 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
dwd_oride_anti_fraud_log_di_prev_day_tesk=HivePartitionSensor(
      task_id="dwd_oride_anti_fraud_log_di_prev_day_task",
      table="log_anti_ofood_oride_fraud",
      partition="dt='{{macros.ds_add(ds, +1)}}' and hour='00'",
      schema="oride_source",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwd_oride_anti_fraud_log_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 


dwd_oride_anti_fraud_log_di_task = HiveOperator(

    task_id='dwd_oride_anti_fraud_log_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
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
 from  oride_source.log_anti_ofood_oride_fraud where dt='{pt}' and action in ('UserRegister','UserLogin','DriverLogin','SilenceUser','UserCancelOrder','DriverCancelOrder','OrderWait','SilenceDriver','OrderArrive','SilenceDriver2','SilenceDriver3','OrderFinish','IsUserSilent','IsDriverSilent')

 '''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
    dag=dag
    )

#生成_SUCCESS
touchz_data_success = BashOperator(

    task_id='touchz_data_success',

    bash_command="""
    line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`
    
    if [ $line_num -eq 0 ]
    then
        echo "FATAL {hdfs_data_dir} is empty"
        exit 1
    else
        echo "DATA EXPORT Successed ......"
        $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
    fi
    """.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        hdfs_data_dir=hdfs_path+'/country_code=nal/dt={{ds}}'
        ),
    dag=dag)
 
dwd_oride_anti_fraud_log_di_prev_day_tesk>>dwd_oride_anti_fraud_log_di_task>>touchz_data_success