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

dag = airflow.DAG('dm_oride_driver_base_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------## 


# 依赖前一天分区
dependence_dim_oride_driver_audit_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_audit_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_audit_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_dwm_oride_driver_audit_third_extend_di_prev_day_task = UFileSensor(
    task_id='dwm_oride_driver_audit_third_extend_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_audit_third_extend_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_order_push_driver_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_push_driver_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_oride_driver_timerange_prev_day_task = HivePartitionSensor(
    task_id="oride_driver_timerange_prev_day_task",
    table="ods_log_oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_click_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_click_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_show_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dm_oride_driver_base_cube_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------## 

dm_oride_driver_base_cube_d_task = HiveOperator(

    task_id='dm_oride_driver_base_cube_d_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    
    SELECT nvl(product_id,-10000) AS product_id,
           nvl(city_id,-10000) AS city_id,
           reg_driver_num,
           --注册司机数
    
           wait_audit_driver_num,
           --待审核司机数
    
           audit_in_driver_num,
           --审核中司机数
    
           audit_finish_driver_num,
           --审核通过司机数
    
           audit_fail_driver_num,
           --审核失败司机数
    
           bind_finish_driver_num,
           --绑定成功司机数
    
           n_bind_driver_num,
           --未绑定司机数
    
           online_driver_num,
           --在线司机数
           
           driver_accept_take_num,
            --骑手应答的总次数 （accept阶段）
            
           driver_take_num,
           --骑手成功应答的总次数 （push阶段）
    
           request_driver_num,
           --当天接单司机数
    
           finish_order_driver_num,
           --当天完单司机数
           
           push_accpet_show_driver_num,
           --被推送骑手数 （accept_show阶段）

           td_reg_driver_num,
           --当天注册司机数
           td_wait_audit_driver_num,
           --当天待审核司机数
           td_audit_in_driver_num,
           --当天审核中司机数
           td_audit_finish_driver_num,
           --当天审核通过司机数
           td_audit_fail_driver_num,
           --当天审核失败司机数
            
           country_code,
           --国家码字段
    
           '{pt}' AS dt
    FROM
      (SELECT dri.product_id,
              dri.city_id,
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
              --当天在线司机数
              
              count(DISTINCT (CASE WHEN ord.driver_id=r1.driver_id THEN ord.driver_id ELSE NULL END)) AS driver_accept_take_num,
              --骑手应答的总次数 （accept_click阶段）
    
              count(DISTINCT (CASE WHEN ord.driver_id=p1.driver_id THEN ord.driver_id ELSE NULL END)) AS driver_take_num,
              --骑手成功应答的总次数 （push阶段）
    
              count(DISTINCT (CASE WHEN is_td_request=1 THEN ord.driver_id ELSE NULL END)) AS request_driver_num,
              --当天接单司机数
    
              count(DISTINCT (CASE WHEN is_td_finish=1 THEN ord.driver_id ELSE NULL END)) AS finish_order_driver_num,
              --当天完单司机数
              
              count(DISTINCT (CASE WHEN ord.driver_id = r2.driver_id THEN ord.driver_id ELSE NULL END)) AS push_accpet_show_driver_num,
              --被推送骑手数 （accept_show阶段）

              count(DISTINCT (CASE WHEN veri_audit_date=dri.dt and dri.driver_id<>0 THEN dri.driver_id ELSE NULL END)) AS td_reg_driver_num,
              --当天注册司机数
    
              count(DISTINCT (CASE WHEN veri_audit_date=dri.dt and dri.driver_id<>0
                              AND dri.status=0 THEN dri.driver_id ELSE NULL END)) AS td_wait_audit_driver_num,
              --当天待审核司机数
    
              count(DISTINCT (CASE WHEN veri_audit_date=dri.dt and dri.driver_id<>0
                              AND dri.status=1 THEN dri.driver_id ELSE NULL END)) AS td_audit_in_driver_num,
              --当天审核中司机数
    
              count(DISTINCT (CASE WHEN veri_audit_date=dri.dt and dri.driver_id<>0
                              AND dri.status=2 THEN dri.driver_id ELSE NULL END)) AS td_audit_finish_driver_num,
              --当天审核通过司机数
    
              count(DISTINCT (CASE WHEN veri_audit_date=dri.dt and dri.driver_id<>0
                              AND dri.status=9 THEN dri.driver_id ELSE NULL END)) AS td_audit_fail_driver_num,
              --当天审核失败司机数
    
              nvl(dri.country_code,-999) AS country_code --(去除with cube为空的BUG) --国家码字段
    
       FROM
         (
            SELECT 
            *
            FROM oride_dw.dim_oride_driver_audit_base
            WHERE dt='{pt}'
            AND city_id<>'999001' --去除测试数据
             and driver_id<>1) dri
       LEFT OUTER JOIN
         (
            SELECT 
            *
            FROM oride_dw.dwd_oride_order_base_include_test_di
             WHERE dt='{pt}'
             AND city_id<>'999001' --去除测试数据
             and driver_id<>1
         ) ord ON dri.driver_id = ord.driver_id
            AND dri.dt = ord.dt
       LEFT OUTER JOIN
         (
            SELECT 
            *
            FROM oride_dw.dwm_oride_driver_audit_third_extend_di
            WHERE dt='{pt}'
          ) ext ON dri.driver_id = ext.driver_id
       AND dri.dt=ext.dt
       LEFT OUTER JOIN
         (
            SELECT 
            *
            FROM oride_dw_ods.ods_log_oride_driver_timerange
            WHERE dt='{pt}'
         ) dtr ON dri.driver_id = dtr.driver_id
       AND dri.dt=dtr.dt
         LEFT OUTER JOIN
      (
           SELECT 
           driver_id --成功播单司机
           FROM oride_dw.dwd_oride_order_push_driver_detail_di
           WHERE dt='{pt}'
           AND success=1
           GROUP BY driver_id
       ) p1 ON ord.driver_id=p1.driver_id
       LEFT OUTER JOIN 
       (
           SELECT 
           driver_id
           FROM 
           oride_dw.dwd_oride_driver_accept_order_click_detail_di
           WHERE dt='{pt}'
           group by driver_id 
       ) r1 on r1.driver_id = dri.driver_id
       LEFT OUTER JOIN 
       (
           SELECT 
           driver_id
           FROM 
           oride_dw.dwd_oride_driver_accept_order_show_detail_di
           WHERE dt='{pt}'
           group by driver_id 
       ) r2 on r2.driver_id = dri.driver_id
       
       GROUP BY dri.product_id,
                dri.city_id,
                dri.country_code 
                WITH CUBE) x
    WHERE x.country_code IN ('nal')
 
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
    ),
    dag=dag)

# 生成_SUCCESS
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
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}'
    ),
    dag=dag)

dependence_dim_oride_driver_audit_base_prev_day_task >> \
dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dwm_oride_driver_audit_third_extend_di_prev_day_task >> \
dependence_dwd_oride_order_push_driver_detail_di_prev_day_task >> \
dependence_oride_driver_timerange_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task >> \
sleep_time >> \
dm_oride_driver_base_cube_d_task >> \
touchz_data_success
