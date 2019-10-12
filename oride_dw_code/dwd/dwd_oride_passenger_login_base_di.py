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

dag = airflow.DAG( 'dwd_oride_passenger_login_base_di', 
    schedule_interval="30 01 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
oride_client_event_detail_prev_day_task=HivePartitionSensor(
      task_id="oride_client_event_detail_prev_day_task",
      table="dwd_oride_client_event_detail_hi",
      partition="""dt='{{ ds }}' and hour='23'""",
      schema="oride_dw",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwd_oride_passenger_login_base_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 


dwd_oride_passenger_login_base_di_task = HiveOperator(

    task_id='dwd_oride_passenger_login_base_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT user_id AS passenger_id,
       --乘客ID

       user_number AS passenger_number,
       --乘客名称

       client_timestamp,
       --客户端向服务器提交事件日志时的时间戳，单位：秒，类似：1469687326

       platform,
       --操作系统平台，取值为Android或者iOS

       os_version,
       --操作系统版本，类似6.0, 9.1.2等

       app_name,
       --客户端名称，乘客端为“oride”，骑手端为“ORide Driver”

       app_version,
       --客户端版本号，比如5.1.4这种类型

       locale,
       --系统使用的语言，格式为：语言代码-区域代码，比如为zh-CN, en-US, en-CA, 语言代码小写，区域代码大写

       device_id,
       --设备id，用于唯一区分设备使用，如果用户卸载，再重新安装，尽量保持同一设备的device_id一样

       device_screen,
       --设备屏幕分辨率，类似1080x1920

       device_model,
       --设备型号，可以具体区分是哪种设备，比如iPhone6, iPhone6s

       device_manufacturer,
       --设备生产商

       is_root,
       --ios是否越狱/android是否root, 两种取值 y: 已经越狱或root；n: 没有越狱或root

       channel,
       --渠道编号，用于区分是哪个渠道带来的安装，跟AppsFlyer相关

       subchannel,
       --子渠道编号，用于区分是哪个渠道带来的安装，跟AppsFlyer相关

       appsflyer_id,
       --appsflyer的唯一标示

       'nal' AS country_code,
       --国家码字段

       dt
FROM oride_bi.oride_client_event_detail
WHERE dt='{pt}'
  AND event_name='oride_show'
  AND app_name='oride'
GROUP BY user_id,
         user_number,
         client_timestamp,
         platform,
         os_version,
         app_name,
         app_version,
         locale,
         device_id,
         device_screen,
         device_model,
         device_manufacturer,
         is_root,
         channel,
         subchannel,
         appsflyer_id,
         dt;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
    dag=dag)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1)-count(distinct passenger_id,passenger_number,client_timestamp,
         platform,
         os_version,
         app_name,
         app_version,
         locale,
         device_id,
         device_screen,
         device_model,
         device_manufacturer,
         is_root,
         channel,
         subchannel,
         appsflyer_id) as cnt
      FROM oride_dw.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
        )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] >1:
        raise Exception ("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")
    
 
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
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

oride_client_event_detail_prev_day_task>>sleep_time>>dwd_oride_passenger_login_base_di_task>>task_check_key_data>>touchz_data_success



