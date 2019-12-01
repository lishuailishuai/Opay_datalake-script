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


dag = airflow.DAG('dwd_opay_user_transfer_user_record_di',
                 schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##
dim_opay_user_base_di_prev_day_task = UFileSensor(
    task_id='dim_opay_user_base_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dim_opay_user_base_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
ods_sqoop_base_user_transfer_user_record_di_prev_day_task = UFileSensor(
    task_id='ods_sqoop_base_user_transfer_user_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay_dw_sqoop_di/opay_transaction/user_transfer_user_record",
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
db_name = "opay_dw"
table_name = "dwd_opay_user_transfer_user_record_di"
hdfs_path="ufile://opay-datalake/opay/opay_dw/" + table_name


def dwd_opay_user_transfer_user_record_di_sql_task(ds):
    HQL='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true;
     
    with user_data as(
        select 
            user_id, `role`
        from (
            select 
                user_id, `role`,
                row_number() over(partition by user_id order by update_time desc) rn
            from opay_dw_ods.ods_sqoop_base_user_di
            where dt <= '{pt}'
        ) t1 where rn = 1
    )
    insert overwrite table {db}.{table} 
    partition(country_code, dt)
    select 
        order_di.id,
        order_di.order_no,
        order_di.user_id,
        if(order_di.create_time < nvl(user_di.agent_upgrade_time, '9999-01-01 00:00:00'), 'customer', 'agent') user_role,
        order_di.user_name,
        order_di.recipient_id,
        case
            when if(order_di.recipient_type='MERCHANT', true, false) then 'merchant'
            when if(order_di.recipient_type='USER', true, false) then recipient_di.role
        end as recipient_role,
        order_di.recipient_name,
        order_di.recipient_mobile,
        order_di.message,
        order_di.amount,
        order_di.country,
        order_di.currency,
        order_di.transfer_status,
        order_di.fee_amount,
        order_di.fee_pattern,
        order_di.outWardId outward_id,
        order_di.outWardType outward_type,
        order_di.error_msg,
        order_di.create_time,
        order_di.update_time,
        order_di.recipient_opay_account,
        order_di.recipient_type,
        case 
            when if(order_di.create_time < nvl(user_di.agent_upgrade_time, '9999-01-01 00:00:00') and order_di.recipient_type='MERCHANT', true, false) then 'c2m'
            when if(order_di.create_time >= nvl(user_di.agent_upgrade_time, '9999-01-01 00:00:00') and order_di.recipient_type='MERCHANT', true, false) then 'a2m'
            when if(order_di.create_time < nvl(user_di.agent_upgrade_time, '9999-01-01 00:00:00') and order_di.create_time < nvl(recipient_di.agent_upgrade_time, '9999-01-01 00:00:00'), true, false) then 'c2c'
            when if(order_di.create_time < nvl(user_di.agent_upgrade_time, '9999-01-01 00:00:00') and order_di.create_time >= nvl(recipient_di.agent_upgrade_time, '9999-01-01 00:00:00'), true, false) then 'c2a'
            when if(order_di.create_time >= nvl(user_di.agent_upgrade_time, '9999-01-01 00:00:00') and order_di.create_time < nvl(recipient_di.agent_upgrade_time, '9999-01-01 00:00:00'), true, false) then 'a2c'
            when if(order_di.create_time >= nvl(user_di.agent_upgrade_time, '9999-01-01 00:00:00') and order_di.create_time >= nvl(recipient_di.agent_upgrade_time, '9999-01-01 00:00:00'), true, false) then 'a2a'
            else 'unknow'
        end as payment_relation_id,
        case order_di.country
            when 'NG' then 'NG'
            when 'NO' then 'NO'
            when 'GH' then 'GH'
            when 'BW' then 'BW'
            when 'GH' then 'GH'
            when 'KE' then 'KE'
            when 'MW' then 'MW'
            when 'MZ' then 'MZ'
            when 'PL' then 'PL'
            when 'ZA' then 'ZA'
            when 'SE' then 'SE'
            when 'TZ' then 'TZ'
            when 'UG' then 'UG'
            when 'US' then 'US'
            when 'ZM' then 'ZM'
            when 'ZW' then 'ZW'
            else 'NG'
            end as country_code,
        order_di.dt
    from 
    (
        select 
            id,
            order_no,
            user_id,
            user_name,
            recipient_id,
            recipient_name,
            recipient_mobile,
            message,
            amount,
            country,
            currency,
            transfer_status,
            fee_amount,
            fee_pattern,
            outwardid,
            outwardtype,
            error_msg,
            create_time,
            update_time,
            recipient_opay_account,
            recipient_type,
            business_type,
            dt
        from
        opay_dw_ods.ods_sqoop_base_user_transfer_user_record_di 
        where dt='{pt}'
    ) order_di 
    left join user_data user_di on user_di.user_id = order_di.user_id
    left join user_data recipient_di on recipient_di.user_id = order_di.recipient_id
    
    '''.format(
        pt=ds,
        db=db_name,
        table=table_name
    )
    return HQL

def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opay_user_transfer_user_record_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_opay_user_transfer_user_record_di_task = PythonOperator(
    task_id='dwd_opay_user_transfer_user_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opay_user_base_di_prev_day_task >> dwd_opay_user_transfer_user_record_di_task
ods_sqoop_base_user_transfer_user_record_di_prev_day_task >> dwd_opay_user_transfer_user_record_di_task