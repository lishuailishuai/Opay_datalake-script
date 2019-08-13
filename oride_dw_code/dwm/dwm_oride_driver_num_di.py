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

dag = airflow.DAG( 'dwm_oride_driver_num_di', 
    schedule_interval="00 01 * * *", 
    default_args=args) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 


#依赖前一天分区
dim_oride_driver_audit_base_prev_day_tesk=UFileSensor(
    task_id='dim_oride_driver_audit_base_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_audit_base/country_code=nal",
        pt='{{ds}}'
        ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
    dag=dag
        )

#依赖前一天分区
dwm_oride_driver_audit_third_extend_di_prev_day_tesk=UFileSensor(
    task_id='dwm_oride_driver_audit_third_extend_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_audit_third_extend_di/country_code=nal",
        pt='{{ds}}'
        ),
    bucket_name='opay-datalake',
    poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
    dag=dag
        )


#依赖前一天分区
oride_driver_timerange_prev_day_tesk=HivePartitionSensor(
      task_id="oride_driver_timerange_prev_day_tesk",
      table="oride_driver_timerange",
      partition="dt='{{ds}}'",
      schema="oride_bi",
      poke_interval=60, #依赖不满足时，一分钟检查一次依赖状态
      dag=dag
    )

##----------------------------------------- 变量 ---------------------------------------## 

table_name="dwm_oride_driver_num_di"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 脚本 ---------------------------------------## 

dwm_oride_driver_num_di_task = HiveOperator(

    task_id='dwm_oride_driver_num_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
SELECT dri.product_id,
       dri.city_id,
       dri.driver_id,
       count(DISTINCT (CASE WHEN dri.driver_id<>0 THEN dri.driver_id ELSE NULL END)) AS reg_driver_num,
       --注册司机数

       count(DISTINCT (CASE WHEN dri.driver_id<>0
                       AND dri.status=0 THEN dri.driver_id ELSE NULL END)) AS wait_audit_driver_num,
       --待审核司机数

       count(DISTINCT (CASE WHEN dri.driver_id<>0
                       AND dri.status=1 THEN dri.driver_id ELSE NULL END)) AS audit_in_driver_num,
       --审核中司机数

       count(DISTINCT (CASE WHEN dri.driver_id<>0
                       AND dri.status=2 THEN dri.driver_id ELSE NULL END)) AS audit_finish_driver_num,
       --审核通过司机数

       count(DISTINCT (CASE WHEN dri.driver_id<>0
                       AND dri.status=9 THEN dri.driver_id ELSE NULL END)) AS audit_fail_driver_num,
       --审核失败司机数

       count(DISTINCT (CASE WHEN dri.driver_id=ext.driver_id
                       AND is_bind=1 THEN ext.driver_id ELSE NULL END)) AS bind_finish_driver_num,
       --绑定成功司机数

       count(DISTINCT (CASE WHEN dri.driver_id=ext.driver_id
                       AND is_bind=0 THEN ext.driver_id ELSE NULL END)) AS n_bind_driver_num,
       --未绑定司机数

       count(DISTINCT (CASE WHEN dri.driver_id=dtr.driver_id
                       AND dri.status=2 THEN dtr.driver_id ELSE NULL END)) AS online_driver_num,
       --在线司机数

       'nal' AS country_code,
       --国家码字段

       '{pt}' AS dt
FROM
  (SELECT *
   FROM oride_dw.dim_oride_driver_audit_base
   WHERE dt='{pt}') dri
LEFT OUTER JOIN
  (SELECT *
   FROM oride_dw.dwm_oride_driver_audit_third_extend_di
   WHERE dt='{pt}') ext ON dri.driver_id=ext.driver_id
AND dri.dt=ext.dt
LEFT OUTER JOIN
  (SELECT *
   FROM oride_bi.oride_driver_timerange
   WHERE dt='{pt}') dtr ON dri.driver_id=dtr.driver_id
AND dri.dt=dtr.dt
GROUP BY dri.product_id,
         dri.city_id,
         dri.driver_id

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

dim_oride_driver_audit_base_prev_day_tesk>>dwm_oride_driver_audit_third_extend_di_prev_day_tesk>>oride_driver_timerange_prev_day_tesk>>sleep_time>>dwm_oride_driver_num_di_task>>touchz_data_success