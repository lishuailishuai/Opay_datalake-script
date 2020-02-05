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
from airflow.sensors import OssSensor
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

dag = airflow.DAG('dim_opos_bd_info_df',
                  schedule_interval="00 01 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##


ods_sqoop_base_bd_admin_users_df_task = OssSensor(
    task_id='ods_sqoop_base_bd_admin_users_df_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_admin_users",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_bd_city_df_task = OssSensor(
    task_id='ods_sqoop_base_bd_city_df_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_city",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "dim_opos_bd_info_df"
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds),
         "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dim_opos_bd_info_df_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;


--01.首先清空组织机构层级临时表,然后插入最新的组织机构层级数据
--插入最新的数据
insert overwrite table opos_dw.dim_opos_bd_info_df partition(country_code,dt)
select 
nvl(hcm.id,0) as hcm_id
,nvl(hcm.name,'-') as hcm_name
,nvl(level2.cm_id,0) as cm_id
,nvl(level2.cm_name,'-') as cm_name
,nvl(level2.rm_id,0) as rm_id
,nvl(level2.rm_name,'-') as rm_name
,nvl(level2.bdm_id,0) as bdm_id
,nvl(level2.bdm_name,'-') as bdm_name
,nvl(level2.bd_id,0) as bd_id
,nvl(level2.bd_name,'-') as bd_name 

,'nal' as country_code
,'{pt}' as dt
from
  (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 2) hcm
full join
  (select nvl(cm.leader_id,0) as leader_id,nvl(cm.id,0) as cm_id,nvl(cm.name,'-') as cm_name,nvl(level3.rm_id,0) as rm_id,nvl(level3.rm_name,'-') as rm_name,nvl(level3.bdm_id,0) as bdm_id,nvl(level3.bdm_name,'-') as bdm_name,nvl(level3.bd_id,0) as bd_id,nvl(level3.bd_name,'-') as bd_name from
    (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 3) cm
  full join
    (select nvl(rm.leader_id,0) as leader_id,nvl(rm.id,0) as rm_id,nvl(rm.name,'-') as rm_name,nvl(level4.bdm_id,0) as bdm_id,nvl(level4.bdm_name,'-') as bdm_name,nvl(level4.bd_id,0) as bd_id,nvl(level4.bd_name,'-') as bd_name from
      (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 4) rm
    full join
      (select nvl(bdm.leader_id,0) as leader_id,nvl(bdm.id,0) as bdm_id,nvl(bdm.name,'-') as bdm_name,nvl(bd.id,0) as bd_id,nvl(bd.name,'-') as bd_name from
        (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 6) bd
      full join
        (select id,name,leader_id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}' and job_id = 5) bdm
      on bdm.id=bd.leader_id) as level4
    on rm.id=level4.leader_id) as level3
  on cm.id=level3.leader_id) as level2
on hcm.id=level2.leader_id;



'''.format(
        pt=ds,
        before_1_day=airflow.macros.ds_add(ds, -1),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL



# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dim_opos_bd_info_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dim_opos_bd_info_df_task = PythonOperator(
    task_id='dim_opos_bd_info_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_bd_admin_users_df_task >> dim_opos_bd_info_df_task
ods_sqoop_base_bd_city_df_task >> dim_opos_bd_info_df_task


