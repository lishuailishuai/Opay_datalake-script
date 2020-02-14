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
                  schedule_interval="10 * * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dwd_oride_client_event_detail_hi"

##----------------------------------------- 依赖 ---------------------------------------##

#获取变量
code_map=eval(Variable.get("sys_flag"))



# 依赖前一小时分区
client_event_prev_hour_task = HivePartitionSensor(
    task_id="client_event_prev_hour_task",
    table="client_event",
    partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


opay_ep_logv0_prev_hour_task = HivePartitionSensor(
    task_id="opay_ep_logv0_prev_hour_task",
    table="opay_ep_logv0",
    partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

opay_ep_logv1_prev_hour_task = HivePartitionSensor(
    task_id="opay_ep_logv1_prev_hour_task",
    table="opay_ep_logv1",
    partition="""dt='{{ ds }}' and hour='{{ execution_date.strftime("%H") }}'""",
    schema="oride_source",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


# 路径
hdfs_path="oss://opay-datalake/oride/oride_dw/"+table_name


##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,execution_date,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}/hour={now_hour}".format(pt=ds,now_hour=execution_date.strftime("%H")), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_client_event_detail_hi_sql_task(ds,hour):
    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        CREATE TEMPORARY FUNCTION from_json AS 'brickhouse.udf.json.FromJsonUDF';
        CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';

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
            null,
            null,
            null,
            null,
            'nal' as country_code,
            dt,
            hour

        FROM
            oride_source.client_event LATERAL VIEW EXPLODE(events) es AS e
        WHERE
            dt='{pt}'
            AND hour='{now_hour}';

        with opay_ep_logv0_data as (
            SELECT
                get_json_object(message, '$.msg') as msg
            FROM
                oride_source.opay_ep_logv0
            WHERE
                dt='{pt}'
                AND hour='{now_hour}'
                AND get_json_object(message, '$.type') in ('driver', 'passenger')
        )
        INSERT INTO TABLE oride_dw.{table} partition(country_code,dt,hour)
        SELECT
            null as ip,
            null as server_ip,
            null as `timestamp`,
            get_json_object(msg, '$.common.user_id'),
            get_json_object(msg, '$.common.user_number'),
            get_json_object(msg, '$.common.client_timestamp'),
            get_json_object(msg, '$.common.platform'),
            get_json_object(msg, '$.common.os_version'),
            get_json_object(msg, '$.common.app_name'),
            get_json_object(msg, '$.common.app_version'),
            get_json_object(msg, '$.common.locale'),
            get_json_object(msg, '$.common.device_id'),
            get_json_object(msg, '$.common.device_screen'),
            get_json_object(msg, '$.common.device_model'),
            get_json_object(msg, '$.common.device_manufacturer'),
            get_json_object(msg, '$.common.is_root'),
            get_json_object(msg, '$.common.channel'),
            get_json_object(msg, '$.common.subchannel'),
            get_json_object(msg, '$.common.gaid'),
            get_json_object(msg, '$.common.appsflyer_id'),
            e.event_time,
            e.event_name,
            e.page,
            e.source,
            e.event_value,
            null,
            null,
            null,
            null,
            'nal' as country_code,
            '{pt}' as dt,
            '{now_hour}' as hour
        FROM
            opay_ep_logv0_data LATERAL VIEW EXPLODE(from_json(get_json_object(msg, '$.events'), array(named_struct("event_name", "", "event_time","", "event_value","", "page","", "source","")))) es AS e;

         with opay_ep_logv1_data as (
            SELECT
                message as msg
            FROM
                oride_source.opay_ep_logv1
            WHERE
                dt='{pt}'
                AND hour='{now_hour}'
                AND get_json_object(message, '$.typ') in ('driver', 'passenger')
        )
        INSERT INTO TABLE oride_dw.{table} partition(country_code,dt,hour)
        SELECT
            null as ip,
            null as server_ip,
            null as `timestamp`,
            get_json_object(msg, '$.uid'),
            get_json_object(msg, '$.uno'),
            get_json_object(msg, '$.t'),
            get_json_object(msg, '$.p'),
            get_json_object(msg, '$.ov'),
            get_json_object(msg, '$.an'),
            get_json_object(msg, '$.av'),
            get_json_object(msg, '$.l'),
            get_json_object(msg, '$.did'),
            get_json_object(msg, '$.dsc'),
            get_json_object(msg, '$.dmo'),
            get_json_object(msg, '$.dma'),
            get_json_object(msg, '$.isr'),
            get_json_object(msg, '$.ch'),
            get_json_object(msg, '$.sch'),
            get_json_object(msg, '$.gaid'),
            get_json_object(msg, '$.aid'),
            e.et,
            e.en,
            null as page,
            null as source,
            to_json(e.ev),
            e.lat,
            e.lng,
            e.cid,
            e.cip,
            'nal' as country_code,
            '{pt}' as dt,
            '{now_hour}' as hour

        FROM
            opay_ep_logv1_data LATERAL VIEW EXPLODE(from_json(get_json_object(msg, '$.es'), array(named_struct("en", "", "et","", "ev", map("",""), "lat","", "lng","","cid", "", "cip", "")))) es AS e
        ;
'''.format(
        pt=ds,
        now_day='{{macros.ds_add(ds, +1)}}',
        now_hour=hour,
        table=table_name,
        db=db_name
        )
    return HQL

#主流程
def execution_data_task_id(ds,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    #读取sql
    _sql=dwd_oride_client_event_detail_hi_sql_task(ds,v_hour)

    logging.info('Executing: %s', _sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据
    #check_key_data_task(ds)

    #生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """

    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"true","true",v_hour)
    
dwd_oride_client_event_detail_hi_task= PythonOperator(
    task_id='dwd_oride_client_event_detail_hi_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

client_event_prev_hour_task >> opay_ep_logv0_prev_hour_task >> opay_ep_logv1_prev_hour_task >> dwd_oride_client_event_detail_hi_task
