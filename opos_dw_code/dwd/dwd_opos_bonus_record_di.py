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


--插入数据
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

set mapred.max.split.size=256000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

set hive.merge.mapfiles = true;
set hive.merge.mapredfiles = true;
set hive.merge.size.per.task = 256000000;
set hive.merge.smallfiles.avgsize=16000000;

--01.先取出管理者是bd的数据
--创建临时表
with 
bd as (
select
  o.id
  ,o.activity_id
  
  ,6 as job_id
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(b.cm_id as int) as cm_id
  ,b.cm_name
  ,cast(b.rm_id as int) as rm_id
  ,b.rm_name
  ,cast(b.bdm_id as int) as bdm_id
  ,b.bdm_name
  ,cast(o.bd_id as int) as bd_id
  ,b.bd_name
  
  ,o.city_id
  
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
  (select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt='{pt}' and bd_id> 0) as o
  inner join
  (select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}') as b
  on
  o.bd_id=b.bd_id
),

--02.统计负责人是bdm的
bdm as (
select
  o.id
  ,o.activity_id
  
  ,5 as job_id
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(b.cm_id as int) as cm_id
  ,b.cm_name
  ,cast(b.rm_id as int) as rm_id
  ,b.rm_name
  ,cast(o.bd_id as int) as bdm_id
  ,b.bdm_name
  ,cast(b.bd_id as int) as bd_id
  ,b.bd_name
  
  ,o.city_id
  
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
  (select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt='{pt}' and bd_id> 0) as o
  inner join
  (select hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,0 as bd_id,'-' as bd_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name) as b
  on
  o.bd_id=b.bdm_id
),

--03.统计负责人是rm的
rm as (
select
  o.id
  ,o.activity_id
  
  ,4 as job_id
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(b.cm_id as int) as cm_id
  ,b.cm_name
  ,cast(o.bd_id as int) as rm_id
  ,b.rm_name
  ,cast(b.bdm_id as int) as bdm_id
  ,b.bdm_name
  ,cast(b.bd_id as int) as bd_id
  ,b.bd_name
  
  ,o.city_id

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
  ,'{pt}' as dt
  from
  (select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt='{pt}' and bd_id> 0) as o
  inner join
  (select hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,0 as bdm_id,'-' as bdm_name,0 as bd_id,'-' as bd_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name) as b
  on
  o.bd_id=b.rm_id
),

--04.统计负责人是cm的
cm as (
select
  o.id
  ,o.activity_id
  
  ,3 as job_id
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(o.bd_id as int) as cm_id
  ,b.cm_name
  ,cast(b.rm_id as int) as rm_id
  ,b.rm_name
  ,cast(b.bdm_id as int) as bdm_id
  ,b.bdm_name
  ,cast(b.bd_id as int) as bd_id
  ,b.bd_name
  
  ,o.city_id
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
  (select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt='{pt}' and bd_id> 0) as o
  inner join
  (select hcm_id,hcm_name,cm_id,cm_name,0 as rm_id,'-' as rm_name,0 as bdm_id,'-' as bdm_name,0 as bd_id,'-' as bd_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name,cm_id,cm_name) as b
  on
  o.bd_id=b.cm_id
),

--05.统计负责人是hcm的
hcm as (
select
  o.id
  ,o.activity_id
  
  ,2 as job_id
  ,cast(o.bd_id as int) as hcm_id
  ,b.hcm_name
  ,cast(b.cm_id as int) as cm_id
  ,b.cm_name
  ,cast(b.rm_id as int) as rm_id
  ,b.rm_name
  ,cast(b.bdm_id as int) as bdm_id
  ,b.bdm_name
  ,cast(b.bd_id as int) as bd_id
  ,b.bd_name
  
  ,o.city_id

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
  (select * from opos_dw_ods.ods_sqoop_base_opos_bonus_record_di where dt='{pt}' and bd_id> 0) as o
  inner join
  (select hcm_id,hcm_name,0 as cm_id,'-' as cm_name,0 as rm_id,'-' as rm_name,0 as bdm_id,'-' as bdm_name,0 as bd_id,'-' as bd_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name) as b
  on
  o.bd_id=b.hcm_id
),

--06.统计关联不上任何bd信息的
nobd as (
select
  o.id
  ,o.activity_id
  
  ,-1 as job_id
  ,0 as hcm_id
  ,'' as hcm_name
  ,0 as cm_id
  ,'' as cm_name
  ,0 as rm_id
  ,'' as rm_name
  ,0 as bdm_id
  ,'' as bdm_name
  ,o.bd_id as bd_id
  ,'' as bd_name
  
  ,o.city_id

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
  (select id from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as b
  on
  o.bd_id=b.id
  where
  b.id is null
)

--07.将所有临时表汇总
insert overwrite table opos_dw.dwd_opos_bonus_record_di partition(country_code,dt)
select 
v1.id
,v1.activity_id

,substr(v1.create_time,0,10) as create_date
,v3.week_of_year as create_week
,substr(v1.create_time,0,7) as create_month
,substr(v1.create_time,0,4) as create_year
  
,v1.job_id
,v1.hcm_id
,v1.hcm_name
,v1.cm_id
,v1.cm_name
,v1.rm_id
,v1.rm_name
,v1.bdm_id
,v1.bdm_name
,v1.bd_id
,v1.bd_name

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
  (
    select * from bd
    union
    select * from bdm
    union
    select * from rm
    union
    select * from cm
    union
    select * from hcm
    union
    select * from nobd
  ) as v1
left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as v2
  on
  v1.city_id=v2.id
left join
  public_dw_dim.dim_date as v3
  on
  substr(v1.create_time,0,10)=v3.dt
left join
  (select * from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as provider
  on
  v1.provider_account=provider.opay_account
left join
  (select * from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as receiver
  on
  v1.receiver_account=receiver.opay_account;



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

