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

dag = airflow.DAG('dm_oride_driver_base_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dim_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dim_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_driver_base/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dwd_oride_order_base_include_test_di_prev_day_tesk = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_oride_order_push_driver_detail_di_prev_day_tesk = UFileSensor(
    task_id='dwd_oride_order_push_driver_detail_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
oride_driver_timerange_prev_day_tesk = HivePartitionSensor(
    task_id="oride_driver_timerange_prev_day_tesk",
    table="ods_log_oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

table_name = "dm_oride_driver_base_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

dm_oride_driver_base_d_task = HiveOperator(

    task_id='dm_oride_driver_base_d_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    
    SELECT product_id,
           city_id,
           driver_finish_order_dur,
           --完单做单时长(分钟）
    
           driver_cannel_pick_dur,
           --取消订单时长（分钟）
    
           driver_free_dur,
           --司机空闲时长（分钟）
    
           succ_push_order_cnt,
           --成功推送司机的订单数
    
           finish_driver_online_dur,
            --完单司机在线时长（分钟）
            
            driver_click_order_cnt,
            --司机点击接受订单总数（accpet_click阶段，算法要求此指标为订单总应答）
            
            driver_pushed_order_cnt,
            --司机被推送订单总数（accpet_show阶段，算法要求此指标为订单总推送）
            
            driver_billing_dur,
            --司机订单计费时长
            strong_finish_driver_online_dur,
            --强派单完单司机在线时长
           country_code,
           --国家码字段
    
           '{pt}' AS dt
    FROM
    (
            SELECT dri.product_id,
            dri.city_id,
            dri.country_code,
            sum(nvl(td_finish_order_dur,0)) AS driver_finish_order_dur,
            --完单做单时长(秒）
    
            sum(nvl(td_cannel_pick_dur,0)) AS driver_cannel_pick_dur,
            --取消订单时长（秒）
    
            sum(nvl(dtr.driver_freerange,0)) AS driver_free_dur,
            --司机空闲时长（秒）
    
            sum(DISTINCT (CASE WHEN ord.driver_id=p1.driver_id THEN ord.succ_push_order_cnt ELSE 0 END)) AS succ_push_order_cnt,--成功推送司机的订单数
    
            sum(if(ord.is_td_finish>=1,nvl(dtr.driver_freerange,0) + nvl(ord.td_finish_order_dur,0) + nvl(ord.td_cannel_pick_dur,0),0)) AS finish_driver_online_dur,
            --完单司机在线时长（秒）
            
            sum(nvl(c1.driver_click_order_cnt,0)) as driver_click_order_cnt,
            --司机点击接受订单总数（accpet_click阶段，算法要求此指标为订单总应答）
            
            sum(nvl(s1.driver_pushed_order_cnt,0)) as driver_pushed_order_cnt,
            --司机被推送订单总数（accpet_show阶段，算法要求此指标为订单总推送）
            
            sum(nvl(ord.td_billing_dur,0)) as driver_billing_dur,
            --司机订单计费时长
            sum(if(ord.is_td_finish>=1 and ord.is_strong_dispatch>=1,nvl(dtr.driver_freerange,0) + nvl(ord.td_finish_order_dur,0) + nvl(ord.td_cannel_pick_dur,0),0)) AS strong_finish_driver_online_dur
            --强派单完单司机在线时长（秒）
    
       FROM
            (
                SELECT 
                *
                FROM oride_dw.dim_oride_driver_base
                WHERE dt='{pt}'
            ) dri
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(order_id) as succ_push_order_cnt,
                sum(if(is_td_finish = 1,td_finish_order_dur,0)) as td_finish_order_dur,
                sum(td_billing_dur) as td_billing_dur,
                sum(td_cannel_pick_dur) as td_cannel_pick_dur,
                sum(is_strong_dispatch) as is_strong_dispatch,  --用于判断该司机是否是强派单司机
                sum(is_td_finish) as is_td_finish  --用于判断该订单是否是完单
                
                FROM oride_dw.dwd_oride_order_base_include_test_di
                WHERE dt='{pt}'
                AND city_id<>'999001' --去除测试数据
                group by driver_id
            ) ord ON dri.driver_id=ord.driver_id
            LEFT OUTER JOIN
            (
                SELECT *
                FROM oride_dw_ods.ods_log_oride_driver_timerange
                WHERE dt='{pt}'
            ) dtr ON dri.driver_id=dtr.driver_id
            AND dri.dt=dtr.dt
            LEFT OUTER JOIN
            (
                SELECT driver_id --成功播单司机
                FROM oride_dw.dwd_oride_order_push_driver_detail_di
                WHERE dt='{pt}'
                AND success=1
                GROUP BY driver_id
            ) p1 ON ord.driver_id=p1.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct(order_id)) driver_click_order_cnt
                FROM 
                oride_dw.dwd_oride_driver_accept_order_click_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) c1 on dri.driver_id=c1.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct(order_id)) driver_pushed_order_cnt
                FROM 
                oride_dw.dwd_oride_driver_accept_order_show_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) s1 on dri.driver_id=s1.driver_id
            
       GROUP BY dri.product_id,
                dri.city_id,
                dri.country_code
    ) x
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

dependence_dim_oride_driver_base_prev_day_task >> dwd_oride_order_base_include_test_di_prev_day_tesk >> dwd_oride_order_push_driver_detail_di_prev_day_tesk >> oride_driver_timerange_prev_day_tesk >> sleep_time >> dm_oride_driver_base_d_task >> touchz_data_success