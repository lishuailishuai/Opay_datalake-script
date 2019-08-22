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

dag = airflow.DAG( 'dwd_ofood_shop_extend_location_df', 
    schedule_interval="00 01 * * *", 
    default_args=args) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 



#依赖前一天分区
ods_sqoop_base_jh_waimai_df_prev_day_tesk=HivePartitionSensor(
      task_id="ods_sqoop_base_jh_waimai_df_prev_day_tesk",
      table="ods_sqoop_base_jh_waimai_df",
      partition="dt='{{ds}}'",
      schema="ofood_dw",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )


##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwd_ofood_shop_extend_location_df"
hdfs_path="ufile://opay-datalake/oride/ofood_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 

dwd_ofood_shop_extend_location_df_task = HiveOperator(

    task_id='dwd_ofood_shop_extend_location_df_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

add jar hdfs://node4.datalake.opay.com:8020/user/hive/warehouse/bigdata_dw/public_tools/pro_dev.jar;
create temporary function getlatlngdata as 'com.udf.dev.BuiLatLngData';

SET hive.exec.parallel=TRUE;
SET hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table ofood_dw.{table} partition(country_code,dt)

SELECT shop_id, --商铺id
       split(lnglat,'\\\\^')[0] AS lat, --纬度
       split(lnglat,'\\\\^')[1] AS lng, --经度
       regexp_extract(min_price_event,"(?<=\\w+:)(.*)(?=\\;\\w+)") AS min_price, --最低金额
       area_polygon, --位置数据源(用于数据校验)
       'nal' as country_code,
        dt
FROM
  (SELECT shop_id,
          area_polygon,
          getlatlngdata(area_polygon) AS client_event,
          regexp_extract(area_polygon,"(?<=min_price\\"\\;)(.*)(?=\\"shipping_fee)") AS min_price_event,
          dt
   FROM ofood_dw.ods_sqoop_base_jh_waimai_df where dt = '{pt}') t1 LATERAL VIEW explode(split(client_event,',')) tt AS lnglat;
 
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
    dag=dag)


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

ods_sqoop_base_jh_waimai_df_prev_day_tesk>>sleep_time>>dwd_ofood_shop_extend_location_df_task>>touchz_data_success