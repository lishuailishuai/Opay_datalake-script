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
from plugins.CountriesAppFrame import CountriesAppFrame
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.sensors import OssSensor

args = {
    'owner': 'lijialong',
    'start_date': datetime(2019, 9, 21),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_skyeye_tableau_d',
                  schedule_interval="00 08 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name = "app_oride_order_skyeye_tableau_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dependence_dwd_oride_order_base_include_test_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_base_include_test_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

dependence_dwd_oride_order_skyeye_di_prev_day_task = OssSensor(
        task_id='dwd_oride_order_skyeye_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_order_skyeye_di/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_order_skyeye_tableau_d_sql_task(ds):
    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite TABLE oride_dw.{table} partition(country_code,dt)

    --天眼系统tableau监控
    select
        bit.city_id,
        count(if(os.is_fraud_order = true,os.order_id,null)) as fraud_order_cnt,--疑似作弊订单量
        count(distinct if(os.is_fraud_order = true,bit.driver_id,null)) as fraud_driver_cnt,--涉及司机人数（一个司机可以接多单）
        count(distinct if(os.is_fraud_order = true,bit.passenger_id,null)) as fraud_passenger_cnt,--涉及乘客人数
        count(if(os.is_fraud_order = true and bit.pay_mode in(2,3),os.order_id,null )) as fraud_online_pay_order_cnt,--线上支付订单量
        count(if(os.is_fraud_order = true and bit.pay_mode in(2,3),os.order_id,null )) / count(if(os.is_fraud_order = true,os.order_id,null)) as fraud_online_pay_order_rio,--线上支付占比
        count(if(bit.status in(4,5),bit.order_id,null)) as completed_num,--大盘完单量 (完单条件，status = 4，5)
        count(if(os.is_fraud_order = true,os.order_id,null)) / count(if(bit.status in(4,5),bit.order_id,null)) as fraud_order_rio,--疑似作弊订单占比
        nvl(bit.country_code,-10000)  as country_code,
        '{pt}'  as dt
    from(   
        select 
            city_id,
            order_id,
            driver_id,
            passenger_id,
            status,
            country_code,
            pay_mode
        from oride_dw.dwd_oride_order_base_include_test_di
        where dt = '{pt}' and city_id != 999001 and  driver_id != 1  
    )bit
    left join 
    (
        select 
            order_id,
            is_fraud_order
        from oride_dw.dwd_oride_order_skyeye_di
        where dt = '{pt}' and order_id is not null
    )os on bit.order_id = os.order_id
    group by bit.city_id,bit.country_code;    
'''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL

#主流程
def execution_data_task_id(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
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
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_order_skyeye_tableau_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

app_oride_order_skyeye_tableau_d_task = PythonOperator(
    task_id='app_oride_order_skyeye_tableau_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

# 执行依赖顺序

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dwd_oride_order_skyeye_di_prev_day_task >> \
app_oride_order_skyeye_tableau_d_task