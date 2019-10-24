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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 10, 7),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_client_event_detail_hi',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 10',
    dag=dag)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一小时分区
dependence_client_event_prev_hour_task = HivePartitionSensor(
    task_id="dependence_client_event_prev_hour_task",
    table="client_event",
    partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##


table_name = "dwd_oride_client_event_detail_hi"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}/hour={{ execution_date.strftime("%H") }}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

dwd_oride_client_event_detail_hi_task = HiveOperator(
    task_id='dwd_oride_client_event_detail_hi_task',

    hql='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt,hour)

        SELECT
            ip,
            server_ip,
            `timestamp`,
            common.user_id,
            common.user_number,
            common.client_timestamp,
            common.platform,
            common.os_version,
            common.app_name,
            common.app_version,
            common.locale,
            common.device_id,
            common.device_screen,
            common.device_model,
            common.device_manufacturer,
            common.is_root,
            common.channel,
            common.subchannel,
            common.gaid,
            common.appsflyer_id,
            e.event_time,
            e.event_name,
            e.page,
            e.source,
            e.event_value,
            'nal' as country_code,
            dt,
            hour

        FROM
            oride_source.client_event LATERAL VIEW EXPLODE(events) es AS e
        WHERE
            dt='{pt}'
            AND hour='{now_hour}'
        
        ;
'''.format(
        pt='{{ds}}',
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name
    ),
    dag=dag
)

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
        hdfs_data_dir=hdfs_path + '/country_code=nal/dt={{ds}}/hour={{ execution_date.strftime("%H") }}'
    ),
    dag=dag)

dependence_client_event_prev_hour_task >> sleep_time >> dwd_oride_client_event_detail_hi_task >> touchz_data_success
