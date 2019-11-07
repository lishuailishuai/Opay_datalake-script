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

##
# 央行月报汇报指标
#
args = {
    'owner': 'xiedong',
    'start_date': datetime(2019, 11, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}


dag = airflow.DAG('dwm_opay_consumption_relation_di',
                  schedule_interval="0 3 2 * *",
                  default_args=args)


##---- hive operator ---##
fill_dwm_opay_consumption_relation_di_task = HiveOperator(
    task_id='fill_dwm_opay_consumption_relation_di_task',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table dwm_opay_consumption_relation_di 
    partition(country_code, dt)
    select
        t1.payment_relation_id, t2.name payment_relation_name, t2.payment_relation_type, t2.role_relation_type, 'Consumption' service_type, t1.order_status, t1.order_amt, t1.order_cnt, 
        t1.country_code, t1.dt
    from
    (
        select 
            payment_relation_id, country_code, dt, sum(amount) order_amt, count(*) order_cnt, order_status 
        from opay_dw.dwd_opay_business_collection_record_di
        group by country_code, dt, payment_relation_id, order_status
    ) t1
    join
    (
        select * from opay_dw.dim_opay_payment_relation_df where dt = '${dt}'
    ) t2
    on t1.payment_relation_id = t2.id

    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw',
    dag=dag
)
##---- hive operator end ---##

##---- hive operator ---##
dwm_opay_consumption_relation_di_task = HiveOperator(
    task_id='dwm_opay_consumption_relation_di_task',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table dwm_opay_consumption_relation_di 
    partition(country_code, dt)
    select
        t1.payment_relation_id, t2.name payment_relation_name, t2.payment_relation_type, t2.role_relation_type, 'Consumption' service_type, t1.order_status, t1.order_amt, t1.order_cnt, 
        t1.country_code, t1.dt
    from
    (
        select 
            payment_relation_id, country_code, dt, sum(amount) order_amt, count(*) order_cnt, order_status 
        from opay_dw.dwd_opay_business_collection_record_di
        where dt='${dt}'
        group by country_code, dt, payment_relation_id, order_status
    ) t1
    join
    (
        select * from opay_dw.dim_opay_payment_relation_df where dt = '${dt}'
    ) t2
    on t1.payment_relation_id = t2.id
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw',
    dag=dag
)

dwm_opay_consumption_relation_di_task