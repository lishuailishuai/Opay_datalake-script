# -*- coding: utf-8 -*-
"""
昨日司机完单分布图多城市对比
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors import UFileSensor
from airflow.sensors import WebHdfsSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from utils.connection_helper import get_hive_cursor
from datetime import datetime, timedelta
import re
import logging
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesAppFrame import CountriesAppFrame
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lishuai',
    'start_date': datetime(2019, 10, 26),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_finished_d',
                  schedule_interval="30 3 * * *",
                  default_args=args,
                  )
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_order_finished_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dwm_oride_driver_base_df_task = OssSensor(
        task_id='dwm_oride_driver_base_df_task',
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )
dim_oride_city_task = OssSensor(
        task_id="dim_oride_city_task",
        bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dim_oride_city",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,
        dag=dag
    )

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_order_finished_d_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        
        insert overwrite table {db}.{table} partition(country_code,dt)
            select k2.city_name,
                k1.driver_finish_order_cnt as wdl, --完单量
                count(distinct k1.driver_id) as qss,--完单司机数
                round(sum(k1.driver_finish_online_dur)/(3600*count(distinct k1.driver_id)),2) as avg_online_dur,--平均在线时长
                'nal' AS country_code,--国家码
                k1.dt
            from (
                select a.dt,
                    a.city_id,
	                a.driver_id,
	                a.driver_finish_online_dur,--在线时长
	                nvl(a.driver_finish_order_cnt,0) driver_finish_order_cnt, --完单量
	                a.is_td_finish
	            from oride_dw.dwm_oride_driver_base_df a
	            where a.dt='{pt}'
            ) k1
            left join 
            (
                select city_id,city_name
                from oride_dw.dim_oride_city
                where dt='{pt}'
            ) k2
            on k1.city_id=k2.city_id
            group by k2.city_name,
            k1.dt,
            k1.driver_finish_order_cnt;
    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL

#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql ='''
        select count(1)-count(distinct city_name,wdl) as cnt
        from {db}.{table}
        where dt='{pt}'
        and country_code in ('nal')
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag

#主流程
def execution_data_task_id(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "false",
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
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_order_finished_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

app_oride_order_finished_d_task = PythonOperator(
    task_id='app_oride_order_finished_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_driver_base_df_task >>app_oride_order_finished_d_task
dim_oride_city_task >>app_oride_order_finished_d_task