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
    'owner': 'yuanfeng',
    'start_date': datetime(2019, 11, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_pre_opos_payment_order_bd_di',
                  schedule_interval="10 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区，dim_opos_bd_relation_df表，ufile://opay-datalake/opos/opos_dw/dim_opos_bd_relation_df
dim_opos_bd_relation_df_task = UFileSensor(
    task_id='dim_opos_bd_relation_df_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dim_opos_bd_relation_df",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_pre_opos_payment_order_di_task = UFileSensor(
    task_id='ods_sqoop_base_pre_opos_payment_order_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop_di/pre_ptsp_db/pre_opos_payment_order",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "dwd_pre_opos_payment_order_bd_di"
hdfs_path = "ufile://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dwd_pre_opos_payment_order_bd_di_sql_task(ds):
    HQL = '''

--插入数据
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--插入数据
insert overwrite table opos_dw.dwd_pre_opos_payment_order_bd_di partition(country_code,dt)
select
o.order_id
,o.receipt_id as opay_id
,nvl(s.contact_phone,'-') as shop_phone
,nvl(s.id,0) as shop_id
,nvl(s.city_code,'-') as city_id
,'-' as category
,nvl(s.bd_id,0) as bd_id
,nvl(s.bdm_id,0) as bdm_id
,nvl(s.rm_id,0) as rm_id
,nvl(s.cm_id,0) as cm_id
,nvl(s.hcm_id,0) as hcm_id
,nvl(s.created_at,'2019-10-25') as create_time

,'nal' as country_code
,'{pt}' as dt
from
(select * from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di where dt='{pt}') as o
left join
(select * from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as s
on
o.receipt_id=s.opay_id;


'''.format(
        pt=ds,
        table=table_name,
        now_day=airflow.macros.ds_add(ds, +1),
        before_1_day=airflow.macros.ds_add(ds, -1),
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_pre_opos_payment_order_bd_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwd_pre_opos_payment_order_bd_di_task = PythonOperator(
    task_id='dwd_pre_opos_payment_order_bd_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opos_bd_relation_df_task >> dwd_pre_opos_payment_order_bd_di_task
ods_sqoop_base_pre_opos_payment_order_di_task >> dwd_pre_opos_payment_order_bd_di_task

# 查看任务命令
# airflow list_tasks dwd_pre_opos_payment_order_bd_di -sd /home/feng.yuan/dwd_pre_opos_payment_order_bd_di.py
# 测试任务命令
# airflow test dwd_pre_opos_payment_order_bd_di dwd_pre_opos_payment_order_bd_di_task 2019-11-24 -sd /home/feng.yuan/dwd_pre_opos_payment_order_bd_di.py


