# -*- coding: utf-8 -*-
"""
oride data_trip数据清洗
"""
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
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.CountriesPublicFrame import CountriesPublicFrame
from airflow.sensors import UFileSensor
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 9, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'dwd_oride_order_trip_travel_df',
    schedule_interval="30 02 * * *",
    default_args=args
)

##----------------------------------------- 依赖 ---------------------------------------## 

ods_sqoop_base_data_trip_df_tesk = UFileSensor(
    task_id='ods_sqoop_base_data_trip_df_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="ufile://opay-datalake/oride_dw_sqoop/oride_data/data_trip",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------## 

db_name="oride_dw"
table_name="dwd_oride_order_trip_travel_df"
hdfs_path="ufile://opay-datalake/oride/oride_dw/"+table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 


def dwd_oride_order_trip_travel_df_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=true;
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        INSERT OVERWRITE TABLE oride_dw.{table} PARTITION(country_code, dt) 
        SELECT 
            NVL(id, 0) as travel_id,
            NVL(driver_id, 0) as driver_id,
            NVL(city_id, 0) as city_id,
            NVL(serv_type, 0) as product_id,
            NVL(pax_num, 0) as pax_num,
            NVL(pax_max, 0) as pax_max,
            NVL(duration, 0) as duration,
            NVL(distance, 0) as distance,
            (CASE WHEN price IS NULL THEN 0 ELSE price END) as  price,
            (CASE WHEN reward IS NULL THEN 0 ELSE reward END) as  reward,
            (CASE WHEN tip IS NULL THEN 0 ELSE tip END) as  tip,
            NVL(CAST(order_id AS bigint), 0) as order_id,
            from_unixtime(create_time,'yyyy-MM-dd HH:mm:ss') as create_time,
            NVL(start_time, 0) as start_time,
            NVL(finish_time, 0) as finish_time,
            NVL(cancel_time, 0) as cancel_time,
            NVL(status, 0) as status,
            NVL(pickup_order_id, 0) as pickup_order_id,
            NVL(count_down, 0) as count_down, 
            'nal' AS country_code, 
            dt 
        FROM (SELECT 
                *,
                split(replace(replace(order_ids,'[',''),']',''), ',') AS orders 
            FROM {ods_db}.{ods_table} 
            WHERE dt = '{pt}'
            ) AS t 
            LATERAL VIEW posexplode(orders) d AS pos, order_id 
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
        )
    return HQL
    
 
#熔断数据，如果数据为0，报错
def check_key_data_task(ds):

    cursor = get_hive_cursor()

    #主键重复校验
    check_sql='''
    SELECT count(1)-count(distinct travel_id,order_id) as cnt
      FROM oride_dw.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
        )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()
 
    if res[0] ==0:
        flag=1
        raise Exception ("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag=0
        print("-----> Notice Data Export Success ......")

    return flag


#主流程
def execution_data_task_id(ds,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
        alter语句: alter_partition
        删除分区: delete_partition
        生产success: touchz_success

        #参数
        第一个参数true: 所有国家是否上线。false 没有
        第二个参数true: 数据目录是有country_code分区。false 没有
        第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """

    cf=CountriesPublicFrame("false",ds,db_name,table_name,hdfs_path,"true","true")

    #删除分区
    cf.delete_partition()

    #读取sql
    _sql="\n"+cf.alter_partition()+"\n"+dwd_oride_order_trip_travel_df_sql_task(ds)

    logging.info('Executing: %s',_sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据，如果数据不能为0
    #check_key_data_cnt_task(ds)

    #熔断数据
    check_key_data_task(ds)

    #生产success
    cf.touchz_success()

    
dwd_oride_order_trip_travel_df_task= PythonOperator(
    task_id='dwd_oride_order_trip_travel_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)



ods_sqoop_base_data_trip_df_tesk>>dwd_oride_order_trip_travel_df_task