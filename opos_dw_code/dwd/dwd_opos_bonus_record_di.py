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
    'start_date': datetime(2019, 11, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opos_bonus_record_di',
                  schedule_interval="20 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区，ods_sqoop_base_opos_bonus_record_di表，ufile://opay-datalake/opos_dw_sqoop/opay_crm/bd_admin_users
ods_sqoop_base_opos_bonus_record_di_task = UFileSensor(
    task_id='ods_sqoop_base_opos_bonus_record_di_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_admin_users",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

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

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "dwd_opos_bonus_record_di"
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

def dwd_opos_bonus_record_di_sql_task(ds):
    HQL = '''
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--插入数据
insert overwrite table opos_dw.dwd_opos_bonus_record_di partition(country_code,dt)
select
o.id
,o.activity_id

,substr(o.create_time,0,10) as create_date
,d.week_of_year as create_week
,substr(o.create_time,0,7) as create_month
,substr(o.create_time,0,4) as create_year

,b.cm_id
,b.cm_name
,b.rm_id
,b.rm_name
,b.bdm_id
,b.bdm_name
,o.bd_id
,b.bd_name

,o.city_id
,c.name as city_name
,c.country

,o.device_id
,o.opay_account

,o.provider_account
,o.receiver_account
,o.amount

,o.use_amount
,o.bonus_rate
,o.bonus_amount

,o.status
,o.settle_status
,o.settle_type
,o.reason
,o.risk_id
,o.settle_time
,o.expire_time
,o.use_time
,o.use_date
,o.create_time
,o.update_time

,'nal' as country_code
,o.dt
from
(select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt='{pt}') as o
left join
(select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}') as b
on
o.bd_id=b.bd_id
left join
(select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as c
on
o.city_id=c.id
left join
public_dw_dim.dim_date as d
on
substr(o.create_time,0,10)=d.dt;



'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_opos_bonus_record_di_sql_task(ds)

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


dwd_opos_bonus_record_di_task = PythonOperator(
    task_id='dwd_opos_bonus_record_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_opos_bonus_record_di_task >> dwd_opos_bonus_record_di_task
dim_opos_bd_relation_df_task >> dwd_opos_bonus_record_di_task

# 查看任务命令
# airflow list_tasks dwd_opos_bonus_record_di -sd /root/feng.yuan/dwd_opos_bonus_record_di.py
# 测试任务命令
# airflow test dwd_opos_bonus_record_di dwd_opos_bonus_record_di_task 2019-11-28 -sd /root/feng.yuan/dwd_opos_bonus_record_di.py

