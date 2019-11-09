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

dag = airflow.DAG('dim_opay_payment_relation_df',
                 schedule_interval="00 03 * * *",
                  default_args=args,
                  catchup=False)

table_name = "dim_opay_payment_relation_df"
hdfs_path="ufile://opay-datalake/opay/opay_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
# ods_payment_relation_base_df_task = UFileSensor(
#     task_id='ods_payment_relation_base_df_task',
#     filepath='{hdfs_path_str}/payment_relation_init.data'.format(
#         hdfs_path_str="opay/opay_dw/ods_payment_relation_base_df",
#         pt='{{ds}}'
#     ),
#     bucket_name='opay-datalake',
#     poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
#     dag=dag
# )

##---- hive operator ---##
dim_opay_payment_relation_df_task = HiveOperator(
    task_id='dim_opay_payment_relation_df_task',
    hql='''


    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.parallel=true; --default false

    alter table dim_opay_payment_relation_df drop partition(dt='{pt}');

    alter table dim_opay_payment_relation_df add partition(dt='{pt}');

    
    insert overwrite table opay_dw.dim_opay_payment_relation_df partition(dt)

    select 
        id, name, payment_relation_type, role_relation_type,'{pt}' as dt
    from opay_dw_ods.ods_payment_relation_base_df
    '''.format(
        pt='{{ds}}'
    ),
    schema='opay_dw',
    dag=dag
)
##---- hive operator end ---##

#生成_SUCCESS
def check_success(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"table":"{dag_name}".format(dag_name=dag_ids),"hdfs_path": "{hdfsPath}/dt={pt}".format(pt=ds,hdfsPath=hdfs_path)}
    ]

    TaskTouchzSuccess().set_touchz_success(msg)

touchz_data_success= PythonOperator(
    task_id='touchz_data_success',
    python_callable=check_success,
    provide_context=True,
    dag=dag
)

dim_opay_payment_relation_df_task >> touchz_data_success

