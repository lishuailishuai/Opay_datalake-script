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

dag = airflow.DAG( 'dwd_oride_driver_pick_up_location_di', 
    schedule_interval="00 03 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 

#依赖前一天分区
dwd_oride_driver_pick_up_location_di_prev_day_tesk=HivePartitionSensor(
      task_id="dwd_oride_driver_pick_up_location_di_prev_day_task",
      table="oride_client_event_detail",
      partition="dt='{{ds}}'",
      schema="oride_bi",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwd_oride_driver_pick_up_location_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 

dwd_oride_driver_pick_up_location_di_task = HiveOperator(

    task_id='dwd_oride_driver_pick_up_location_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT driver_id,
       --主键（driver_id,order_id,pick_up_status）

       city_id,
       --定位城市

       product_id,
       --接单司机类型，1=专车，2=快车

       current_point,
       --用户当前位置

       order_id,
       --订单号

       pick_up_status,
       --接驾类型(1 接用户-已接到乘客,2 接用户-到达终点)

       'nal' AS country_code,
       --国家码字段

       '{pt}' AS dt
FROM
  (SELECT user_id AS driver_id,
          get_json_object(event_value, '$.current_point') AS current_point,
          get_json_object(event_value, '$.order_id') AS order_id,
          get_json_object(event_value, '$.city_id') AS city_id,
          get_json_object(event_value, '$.serv_type') AS product_id,
          1 AS pick_up_status
   FROM oride_bi.oride_client_event_detail
   WHERE dt='{pt}'
     AND event_name='start_ride_sliding'
   UNION ALL SELECT user_id AS driver_id,
                    get_json_object(event_value, '$.current_point') AS current_point,
                    get_json_object(event_value, '$.order_id') AS order_id,
                    get_json_object(event_value, '$.city_id') AS city_id,
                    get_json_object(event_value, '$.serv_type') AS product_id,
                    2 AS pick_up_status
   FROM oride_bi.oride_client_event_detail
   WHERE dt='{pt}'
     AND event_name='start_ride_sliding_arrived') t1
GROUP BY driver_id,
         city_id,
         product_id,
         current_point,
         order_id,
         pick_up_status;

'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
schema='oride_dw',
    dag=dag)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1)-count(distinct driver_id,order_id,pick_up_status) as cnt
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

dwd_oride_driver_pick_up_location_di_prev_day_tesk>>sleep_time>>dwd_oride_driver_pick_up_location_di_task>>task_check_key_data>>touchz_data_success


