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


dag = airflow.DAG('dwm_opay_recharge_service_provider_di',
                 schedule_interval="20 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##
#依赖前一天分区
dwd_opay_recharge_betting_record_di_task = UFileSensor(
    task_id='dwd_opay_recharge_betting_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_recharge_betting_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#依赖前一天分区
dwd_opay_recharge_electricity_record_di_task = UFileSensor(
    task_id='dwd_opay_recharge_electricity_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_recharge_electricity_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#依赖前一天分区
dwd_opay_recharge_mobilebill_record_di_task = UFileSensor(
    task_id='dwd_opay_recharge_mobilebill_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_recharge_mobilebill_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#依赖前一天分区
dwd_opay_recharge_mobiledata_record_di_task = UFileSensor(
    task_id='dwd_opay_recharge_mobiledata_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_recharge_mobiledata_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#依赖前一天分区
dwd_opay_recharge_tv_record_di_task = UFileSensor(
    task_id='dwd_opay_recharge_tv_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_recharge_tv_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#依赖前一天分区
dim_opay_service_provider_df_task = UFileSensor(
    task_id='dim_opay_service_provider_df_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_service_provider_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

table_name = "dwm_opay_recharge_service_provider_di"
hdfs_path="ufile://opay-datalake/opay/opay_dw/" + table_name

##---- hive operator ---##
# fill_dwm_opay_recharge_service_provider_di_task = HiveOperator(
#     task_id='fill_dwm_opay_recharge_service_provider_di_task',
#     hql='''
#     set hive.exec.dynamic.partition.mode=nonstrict;
#     insert overwrite table dwm_opay_recharge_service_provider_di
#     partition(country_code, dt)
#     select
# 	    t1.service_provider_id, t2.name service_provider_name, t2.service_type, t2.provider_type, t1.user_role, t1.order_amt, t1.order_cnt, t1.order_status, t1.country_code, t1.dt
#     from
#     (
#         select
#             service_provider_id, user_role, 'TV' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
#         from opay_dw.dwd_opay_recharge_tv_record_di
#         group by country_code, dt, service_provider_id, user_role, order_status
#         union all
#         select
#             service_provider_id, user_role, 'Betting' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
#         from opay_dw.dwd_opay_recharge_betting_record_di
#         group by country_code, dt, service_provider_id, user_role, order_status
#         union all
#         select
#             service_provider_id, user_role, 'MobileData' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
#         from opay_dw.dwd_opay_recharge_mobiledata_record_di
#         group by country_code, dt, service_provider_id, user_role, order_status
#         union all
#         select
#             service_provider_id, user_role, 'Airtime' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
#         from opay_dw.dwd_opay_recharge_mobilebill_record_di
#         group by country_code, dt, service_provider_id, user_role, order_status
#         union all
#         select
#             service_provider_id, user_role, 'Electricity' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
#         from opay_dw.dwd_opay_recharge_electricity_record_di
#         group by country_code, dt, service_provider_id, user_role, order_status
#     ) t1
#     join
#     (
#         select * from dim_opay_service_provider_df where dt = '{pt}'
#     ) t2
#     on t1.service_provider_id = t2.id
#
#     '''.format(
#         pt='{{ds}}'
#     ),
#     schema='opay_dw',
#     dag=dag
# )
##---- hive operator end ---##

##---- hive operator ---##
dwm_opay_recharge_service_provider_di_task = HiveOperator(
    task_id='dwm_opay_recharge_service_provider_di_task',
    hql='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
    insert overwrite table dwm_opay_recharge_service_provider_di 
    partition(country_code, dt)
    select 
        t1.service_provider_id, t2.name service_provider_name, t2.service_type, t2.provider_type, t1.user_role, t1.order_amt, t1.order_cnt, t1.order_status, t1.country_code, t1.dt
    from 
    (
        select 
            service_provider_id, user_role, 'TV' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
        from opay_dw.dwd_opay_recharge_tv_record_di
        where dt='{pt}'
        group by country_code, dt, service_provider_id, user_role, order_status
        union all
        select 
            service_provider_id, user_role, 'Betting' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
        from opay_dw.dwd_opay_recharge_betting_record_di
        where dt='{pt}'
        group by country_code, dt, service_provider_id, user_role, order_status
        union all
        select 
            service_provider_id, user_role, 'MobileData' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
        from opay_dw.dwd_opay_recharge_mobiledata_record_di
        where dt='{pt}'
        group by country_code, dt, service_provider_id, user_role, order_status
        union all
        select 
            service_provider_id, user_role, 'Airtime' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
        from opay_dw.dwd_opay_recharge_mobilebill_record_di
        where dt='{pt}'
        group by country_code, dt, service_provider_id, user_role, order_status
        union all
        select 
            service_provider_id, user_role, 'Electricity' service_type, order_status, sum(amount) order_amt, count(*) order_cnt, country_code, dt
        from opay_dw.dwd_opay_recharge_electricity_record_di
        where dt='{pt}'
        group by country_code, dt, service_provider_id, user_role, order_status
    ) t1 
    join 
    (
        select * from dim_opay_service_provider_df where dt = '{pt}'
    ) t2 
    on t1.service_provider_id = t2.id
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw',
    dag=dag
)

#生成_SUCCESS
def check_success(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"table":"{dag_name}".format(dag_name=dag_ids),"hdfs_path": "{hdfsPath}/country_code=NG/dt={pt}".format(pt=ds,hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)

touchz_data_success= PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dwd_opay_recharge_betting_record_di_task >> dwm_opay_recharge_service_provider_di_task
dwd_opay_recharge_tv_record_di_task >> dwm_opay_recharge_service_provider_di_task
dwd_opay_recharge_mobilebill_record_di_task >> dwm_opay_recharge_service_provider_di_task
dwd_opay_recharge_mobiledata_record_di_task >> dwm_opay_recharge_service_provider_di_task
dwd_opay_recharge_electricity_record_di_task >> dwm_opay_recharge_service_provider_di_task
dim_opay_service_provider_df_task >> dwm_opay_recharge_service_provider_di_task
dwm_opay_recharge_service_provider_di_task >> touchz_data_success