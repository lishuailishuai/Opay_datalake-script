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

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 11, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_driver_track_data_hi',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dwd_driver_track_data_hi"
# 路径
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dependence_ods_log_driver_track_data_hi_task = HivePartitionSensor(
        task_id="dependence_ods_log_driver_track_data_hi_task",
        table="ods_log_driver_track_data_hi",
        partition=""" dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}' """,
        schema="oride_dw_ods",
        poke_interval=60,
        dag=dag
    )


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, execution_date,dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}/hour={hour}".format(pt=ds, hour=execution_date.strftime("%H")), "timeout": "3600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##
def dwd_driver_track_data_hi_sql_task(ds,hour):
    HQL = '''
    SET hive.exec.parallel=TRUE;
   SET hive.exec.dynamic.partition.mode=nonstrict;
   insert overwrite table oride_dw.dwd_driver_track_data_hi partition(country_code,dt,hour)   
   select id as driver_id,
          --司机ID

          order_id as order_id,
          --订单ID

          lng, 
          --经度

          lat,
          --纬度

          direction,
          --方向

          mode,
          --模式

          city_id,
          --城市ID

          `timestamp` as log_timestamp,
          --日志时间

          'nal' as country_code,

          dt,
          
          hour

    from oride_dw_ods.ods_log_driver_track_data_hi
   where dt='{pt}' and hour='{now_hour}';
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        now_hour=hour,
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
            "is_hour_task": "true",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_driver_track_data_hi_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

dwd_driver_track_data_hi_task = PythonOperator(
    task_id='dwd_driver_track_data_hi_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dependence_ods_log_driver_track_data_hi_task >> dwd_driver_track_data_hi_task

