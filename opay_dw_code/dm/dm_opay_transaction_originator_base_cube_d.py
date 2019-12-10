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
    'owner': 'xiedong',
    'start_date': datetime(2019, 11, 11),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_opay_transaction_originator_base_cube_d',
                  schedule_interval="00 03 * * *",
                  default_args=args
                  )

##----------------------------------------- 依赖 ---------------------------------------##
# 依赖前一天分区
dwd_opay_life_payment_record_di_prev_day_task = UFileSensor(
    task_id='dwd_opay_life_payment_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_life_payment_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_cash_to_card_record_di_prev_day_task = UFileSensor(
    task_id='dwd_opay_cash_to_card_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_cash_to_card_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dwd_opay_transfer_of_account_record_di_prev_day_task = UFileSensor(
    task_id='dwd_opay_transfer_of_account_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_transfer_of_account_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
dwd_opay_pos_transaction_record_di_prev_day_task = UFileSensor(
    task_id='dwd_opay_pos_transaction_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_pos_transaction_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_topup_with_card_record_di_prev_day_task = UFileSensor(
    task_id='dwd_opay_topup_with_card_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_topup_with_card_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_easycash_record_di_prev_day_task = UFileSensor(
    task_id='dwd_opay_easycash_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_easycash_record_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_opay_receive_money_record_di_prev_day_task = UFileSensor(
    task_id='dwd_opay_receive_money_record_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/dwd_opay_receive_money_record_di/country_code=NG",
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
db_name="opay_dw"
table_name="dm_opay_transaction_originator_base_cube_d"
hdfs_path="ufile://opay-datalake/opay/opay_dw/"+table_name

##---- hive operator ---##
def dm_opay_transaction_originator_base_cube_d_sql_task(ds):
    HQL='''
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false

    insert overwrite table {db}.{table} partition(dt = '{pt}')
    select 
        nvl(country_code, 'ALL') as country_code,
        nvl(top_service_type, 'ALL') as top_service_type,
        nvl(sub_service_type, 'ALL') as sub_service_type,
        nvl(originator_type, 'ALL') as originator_type,
        nvl(originator_role, 'ALL') as originator_role,
        nvl(order_status, 'ALL') as order_status, 
        sum(amount) order_amt, count(*) order_cnt, count(distinct originator_id) originator_cnt
    from (
        select 
            country_code, top_service_type, sub_service_type, originator_type, originator_role, originator_kyc_level, originator_money_flow, order_status,
            amount, originator_id
        from {db}.dwd_opay_transaction_record_di
        where dt = '{pt}' and create_time BETWEEN date_format(date_sub('{pt}', 1), 'yyyy-MM-dd 23') AND date_format('{pt}', 'yyyy-MM-dd 23')
    ) t1
    group by country_code, top_service_type, sub_service_type, originator_type, originator_role, order_status
    GROUPING SETS ( 
        (country_code, top_service_type, sub_service_type, originator_type, originator_role, order_status),
        (country_code, top_service_type, sub_service_type, originator_type, originator_role),
        (country_code, top_service_type, sub_service_type, originator_type),
        (country_code, top_service_type, sub_service_type),
        (country_code, top_service_type),
        (country_code),
        ()
    )

    '''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL

##---- hive operator end ---##

def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dm_opay_transaction_originator_base_cube_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)



    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "false", "true")

dm_opay_transaction_originator_base_cube_d_task = PythonOperator(
    task_id='dm_opay_transaction_originator_base_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opay_life_payment_record_di_prev_day_task >> dm_opay_transaction_originator_base_cube_d_task
dwd_opay_cash_to_card_record_di_prev_day_task >> dm_opay_transaction_originator_base_cube_d_task
dwd_opay_transfer_of_account_record_di_prev_day_task >> dm_opay_transaction_originator_base_cube_d_task
dwd_opay_topup_with_card_record_di_prev_day_task >> dm_opay_transaction_originator_base_cube_d_task
dwd_opay_receive_money_record_di_prev_day_task >> dm_opay_transaction_originator_base_cube_d_task
dwd_opay_pos_transaction_record_di_prev_day_task >> dm_opay_transaction_originator_base_cube_d_task
dwd_opay_easycash_record_di_prev_day_task >> dm_opay_transaction_originator_base_cube_d_task