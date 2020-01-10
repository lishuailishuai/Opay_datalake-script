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
    'start_date': datetime(2019, 11, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_opos_bonus_record_di',
                  schedule_interval="30 01 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

ods_sqoop_base_opos_bonus_record_di_task = OssSensor(
    task_id='ods_sqoop_base_opos_bonus_record_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_admin_users",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_opos_scan_history_di_task = OssSensor(
    task_id='ods_sqoop_base_opos_scan_history_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop_di/opos_cashback/opos_scan_history",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_opos_bd_relation_df_task = OssSensor(
    task_id='dim_opos_bd_relation_df_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
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
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
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


--插入数据
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;


--07.将所有临时表汇总
insert overwrite table opos_dw.dwd_opos_bonus_record_di partition(country_code,dt)
select 
v1.id
,v1.activity_id

,substr('{pt}',0,10) as create_date
,weekofyear('{pt}') as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year
  
,0 as job_id
,nvl(v4.hcm_id,0) as hcm_id
,nvl(v4.hcm_name,'-') as hcm_name
,nvl(v4.cm_id,0) as cm_id
,nvl(v4.cm_name,'-') as cm_name
,nvl(v4.rm_id,0) as rm_id
,nvl(v4.rm_name,'-') as rm_name
,nvl(v4.bdm_id,0) as bdm_id
,nvl(v4.bdm_name,'-') as bdm_name
,nvl(v4.bd_id,0) as bd_id
,nvl(v4.bd_name,'-') as bd_name

,v1.city_id
,v2.name as city_name
,v2.country

,v1.device_id
,v1.opay_account

,v1.provider_account
,provider.id as provider_shop_id
,provider.opay_id as provider_opay_id
,provider.shop_name as provider_shop_name
,provider.contact_name as provider_contact_name
,provider.contact_phone as provider_contact_phone
,provider.created_at as provider_created_at
,provider.city_code as provider_city_id
,provider.city_name as provider_city_name
,provider.country as provider_country

,v1.receiver_account
,receiver.id as receiver_shop_id
,receiver.opay_id as receiver_opay_id
,receiver.shop_name as receiver_shop_name
,receiver.contact_name as receiver_contact_name
,receiver.contact_phone as receiver_contact_phone
,receiver.created_at as receiver_created_at
,receiver.city_code as receiver_city_id
,receiver.city_name as receiver_city_name
,receiver.country as receiver_country

,v1.amount
,v1.use_amount
,v1.bonus_rate
,v1.bonus_amount
,v1.status
,v1.settle_status
,v1.settle_type
,v1.reason
,v1.risk_id
,v1.settle_time
,v1.expire_time
,v1.use_time
,v1.use_date
,v1.create_time
,v1.update_time

,'nal' as country_code
,v1.dt
from
  (select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt = '{pt}') as v1
left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as v2
on
  v1.city_id=v2.id
left join
  (select * from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as provider
on
  v1.provider_account=provider.opay_account
left join
  (select * from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as receiver
on
  v1.receiver_account=receiver.opay_account
left join
  (
  select
    h.id
    ,h.qr_code
    ,h.bonus_id
  
    ,h.hcm_id
    ,hcm.name as hcm_name
    ,h.cm_id
    ,cm.name as cm_name
    ,h.rm_id
    ,rm.name as rm_name
    ,h.bdm_id
    ,bdm.name as bdm_name
    ,h.bd_id
    ,bd.name as bd_name
  from
    (select id,qr_code,bonus_id,hcm_id,cm_id,rm_id,bdm_id,bd_id from opos_dw_ods.ods_sqoop_base_opos_scan_history_di where dt = '{pt}') as h
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bd
    on h.bd_id=bd.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bdm
    on h.bdm_id=bdm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as rm
    on h.rm_id=rm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as cm
    on h.cm_id=cm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as hcm
    on h.hcm_id=hcm.id
  ) as v4
on
  v1.id=v4.bonus_id
;





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
ods_sqoop_base_opos_scan_history_di_task >> dwd_opos_bonus_record_di_task

# 查看任务命令
# airflow list_tasks dwd_opos_bonus_record_di -sd /root/feng.yuan/dwd_opos_bonus_record_di.py
# 测试任务命令
# airflow test dwd_opos_bonus_record_di dwd_opos_bonus_record_di_task 2019-11-28 -sd /root/feng.yuan/dwd_opos_bonus_record_di.py

