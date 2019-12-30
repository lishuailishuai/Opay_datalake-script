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
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

##
# 央行月报汇报指标
#
args = {
    'owner': 'xiedong',
    'start_date': datetime(2019, 12, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = airflow.DAG('dwd_opay_client_event_base_di',
                 schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##
# 依赖前一天分区
dwd_opay_client_event_base_di_prev_day_task = HivePartitionSensor(
    task_id="dwd_opay_client_event_base_di_prev_day_task",
    table="client_event",
    partition="dt='{{ ds }}' and hour='22'",
    schema="opay_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "opay_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name="opay_dw"
table_name = "dwd_opay_client_event_base_di"
hdfs_path="oss://opay-datalake/opay/opay_dw/" + table_name



def dwd_opay_client_event_base_di_sql_task(ds):
    HQL='''
    
    set mapred.max.split.size=1000000;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table {db}.{table} 
    partition(country_code, dt)
    select 
        ip as client_ip, server_ip, 
        from_unixtime(`timestamp`, 'yyyy-MM-dd HH:mm:ss') as server_time, `timestamp` as server_timestamp,
        common.user_id, common.user_number as mobile, common.city_id, 
        from_unixtime(cast(common.client_timestamp as BIGINT), 'yyyy-MM-dd HH:mm:ss') as client_report_time, cast(common.client_timestamp as BIGINT) as client_report_timestamp,
        common.platform, common.os_version, common.app_name, common.app_version, common.locale, common.device_id, 
        common.device_screen, common.device_model, common.device_manufacturer, common.is_root, common.channel, common.subchannel, 
        common.gaid, common.appsflyer_id, 
        from_unixtime(cast(cast(event.event_time as bigint) / 1000 as bigint), 'yyyy-MM-dd HH:mm:ss') as event_time, 
        cast(cast(event.event_time as bigint) / 1000 as bigint) as event_timestamp,
        event.event_name, event.page, event.source, event.event_value,
        'nal' as country_code, '{pt}' as dt
    from (
      select 
      *
      from opay_source.client_event 
      where (dt = '{pt}' and hour < 23) or (dt=date_sub('{pt}', 1) and hour = 23)
    ) t1 lateral view explode(t1.events) event_value as event;
    
    '''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )
    return HQL


def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opay_client_event_base_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_opay_client_event_base_di_task = PythonOperator(
    task_id='dwd_opay_client_event_base_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_client_event_base_di_prev_day_task >> dwd_opay_client_event_base_di_task