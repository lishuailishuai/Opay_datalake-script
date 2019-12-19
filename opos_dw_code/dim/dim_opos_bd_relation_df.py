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

dag = airflow.DAG('dim_opos_bd_relation_df',
                  schedule_interval="00 02 * * *",
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

ods_sqoop_base_bd_shop_df_task = OssSensor(
    task_id='ods_sqoop_base_bd_shop_df_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop/opay_crm/bd_shop",
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
table_name = "dim_opos_bd_relation_df"
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds),
         "timeout": "6000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dim_opos_bd_relation_df_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

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


--02.取出所有商户信息，关联bd
with
bd as (
--bd
  select
  s.id
  ,s.opay_id as opay_id
  ,s.shop_name as shop_name
  ,s.opay_account as opay_account
  ,s.city_id as city_code
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(b.cm_id as int) as cm_id
  ,b.cm_name
  ,cast(b.rm_id as int) as rm_id
  ,b.rm_name
  ,cast(b.bdm_id as int) as bdm_id
  ,b.bdm_name
  ,cast(s.bd_id as int) as bd_id
  ,b.bd_name
  ,s.contact_name
  ,s.contact_phone
  ,s.cate_id
  ,s.status
  ,substr(s.created_at,0,10) as created_at
  ,s.shop_class
  ,s.bd_id as created_bd_id
  from
    (select * from opos_dw_ods.ods_sqoop_base_bd_shop_df where dt = '{pt}' and bd_id>0) as s
  inner join
    (select * from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}') as b
  on s.bd_id=b.bd_id
  ),


bdm as (  
  --bdm
  select
  s.id
  ,s.opay_id as opay_id
  ,s.shop_name as shop_name
  ,s.opay_account as opay_account
  ,s.city_id as city_code
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(b.cm_id as int) as cm_id
  ,b.cm_name
  ,cast(b.rm_id as int) as rm_id
  ,b.rm_name
  ,cast(s.bd_id as int) as bdm_id
  ,b.bdm_name
  ,0 as bd_id
  ,'-' as bd_name
  ,s.contact_name
  ,s.contact_phone
  ,s.cate_id
  ,s.status
  ,substr(s.created_at,0,10) as created_at
  ,s.shop_class
  ,s.bd_id as created_bd_id
  from
    (select * from opos_dw_ods.ods_sqoop_base_bd_shop_df where dt = '{pt}' and bd_id>0) as s
  inner join
    (select hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name) as b
  on s.bd_id=b.bdm_id
  ),

rm as (
  --rm
  select
  s.id
  ,s.opay_id as opay_id
  ,s.shop_name as shop_name
  ,s.opay_account as opay_account
  ,s.city_id as city_code
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(b.cm_id as int) as cm_id
  ,b.cm_name
  ,cast(s.bd_id as int) as rm_id
  ,b.rm_name
  ,0 as bdm_id
  ,'-' as bdm_name
  ,0 as bd_id
  ,'-' as bd_name
  ,s.contact_name
  ,s.contact_phone
  ,s.cate_id
  ,s.status
  ,substr(s.created_at,0,10) as created_at
  ,s.shop_class
  ,s.bd_id as created_bd_id
  from
    (select * from opos_dw_ods.ods_sqoop_base_bd_shop_df where dt = '{pt}' and bd_id>0) as s
  inner join
    (select hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name) as b
  on s.bd_id=b.rm_id
  ),

cm as (
  --cm
  select
  s.id
  ,s.opay_id as opay_id
  ,s.shop_name as shop_name
  ,s.opay_account as opay_account
  ,s.city_id as city_code
  ,cast(b.hcm_id as int) as hcm_id
  ,b.hcm_name
  ,cast(s.bd_id as int) as cm_id
  ,b.cm_name
  ,0 as rm_id
  ,'-' as rm_name
  ,0 as bdm_id
  ,'-' as bdm_name
  ,0 as bd_id
  ,'-' as bd_name
  ,s.contact_name
  ,s.contact_phone
  ,s.cate_id
  ,s.status
  ,substr(s.created_at,0,10) as created_at
  ,s.shop_class
  ,s.bd_id as created_bd_id
  from
    (select * from opos_dw_ods.ods_sqoop_base_bd_shop_df where dt = '{pt}' and bd_id>0) as s
  inner join
    (select hcm_id,hcm_name,cm_id,cm_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name,cm_id,cm_name) as b
  on s.bd_id=b.cm_id
),

hcm as (
  
  --hcm
  select
  s.id
  ,s.opay_id as opay_id
  ,s.shop_name as shop_name
  ,s.opay_account as opay_account
  ,s.city_id as city_code
  ,cast(s.bd_id as int) as hcm_id
  ,b.hcm_name
  ,0 as cm_id
  ,'-' as cm_name
  ,0 as rm_id
  ,'-' as rm_name
  ,0 as bdm_id
  ,'-' as bdm_name
  ,0 as bd_id
  ,'-' as bd_name
  ,s.contact_name
  ,s.contact_phone
  ,s.cate_id
  ,s.status
  ,substr(s.created_at,0,10) as created_at
  ,s.shop_class
  ,s.bd_id as created_bd_id
  from
    (select * from opos_dw_ods.ods_sqoop_base_bd_shop_df where dt = '{pt}' and bd_id>0) as s
  inner join
    (select hcm_id,hcm_name from opos_dw.dim_opos_bd_info_df where country_code='nal' and dt='{pt}' group by hcm_id,hcm_name) as b
  on s.bd_id=b.hcm_id
 
  ),

nobd as (
  --关联不上的
  select
  s.id
  ,s.opay_id as opay_id
  ,s.shop_name as shop_name
  ,s.opay_account as opay_account
  ,s.city_id as city_code
  ,0 as hcm_id
  ,'-' as hcm_name
  ,0 as cm_id
  ,'-' as cm_name
  ,0 as rm_id
  ,'-' as rm_name
  ,0 as bdm_id
  ,'-' as bdm_name
  ,cast(s.bd_id as int) as bd_id
  ,'-' as bd_name
  ,s.contact_name
  ,s.contact_phone
  ,s.cate_id
  ,s.status
  ,substr(s.created_at,0,10) as created_at
  ,s.shop_class
  ,s.bd_id as created_bd_id
  from
    (select * from opos_dw_ods.ods_sqoop_base_bd_shop_df where dt = '{pt}' and bd_id>0) as s
  left join
    (select id,job_id from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as b
  on s.bd_id=b.id
  where b.job_id is null

)

insert overwrite table opos_dw.opos_dw.dim_opos_bd_relation_tmp_df partition(country_code,dt)
select 
b.id
,b.opay_id
,b.shop_name
,b.opay_account
,b.city_code
,c.name as city_name
,c.country
,b.job_id
,b.hcm_id
,b.hcm_name
,b.cm_id
,b.cm_name
,b.rm_id
,b.rm_name
,b.bdm_id
,b.bdm_name
,b.bd_id
,b.bd_name
,b.contact_name
,b.contact_phone
,b.cate_id
,b.status
,b.created_at
,b.shop_class
,b.created_bd_id

,'nal' as country_code
,'{pt}' as dt 
from
  (select id,opay_id,shop_name,opay_account,city_code,6 as job_id,hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name,contact_name,contact_phone,cate_id,status,created_at,shop_class,created_bd_id from bd
  union
  select id,opay_id,shop_name,opay_account,city_code,5 as job_id,hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name,contact_name,contact_phone,cate_id,status,created_at,shop_class,created_bd_id from bdm
  union
  select id,opay_id,shop_name,opay_account,city_code,4 as job_id,hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name,contact_name,contact_phone,cate_id,status,created_at,shop_class,created_bd_id from rm
  union
  select id,opay_id,shop_name,opay_account,city_code,3 as job_id,hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name,contact_name,contact_phone,cate_id,status,created_at,shop_class,created_bd_id from cm
  union
  select id,opay_id,shop_name,opay_account,city_code,2 as job_id,hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name,contact_name,contact_phone,cate_id,status,created_at,shop_class,created_bd_id from hcm
  union
  select id,opay_id,shop_name,opay_account,city_code,-1 as job_id,hcm_id,hcm_name,cm_id,cm_name,rm_id,rm_name,bdm_id,bdm_name,bd_id,bd_name,contact_name,contact_phone,cate_id,status,created_at,shop_class,created_bd_id from nobd
  ) as b
left join
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as c
on
  b.city_code=c.id
;

--03.将最新的首单交易日期数据插入到最终表中
insert overwrite table opos_dw.dim_opos_bd_relation_df partition(country_code,dt)
select 
m.id
,m.opay_id
,m.shop_name
,m.opay_account
,m.city_code
,m.city_name
,m.country
,m.job_id
,m.hcm_id
,m.hcm_name
,m.cm_id
,m.cm_name
,m.rm_id
,m.rm_name
,m.bdm_id
,m.bdm_name
,m.bd_id
,m.bd_name
,m.contact_name
,m.contact_phone
,m.cate_id
,m.status
,m.created_at
--如果关联上当天交易的商户,说明商户当天是第一笔交易,那就去当天时间作为第一笔,反之还是取历史表中的日期作为第一笔
,if(n.dt is null,m.first_order_date,n.dt) as first_order_date
,m.shop_class
,m.created_bd_id
,o.phone as created_bd_phone
,o.name as created_bd_name
,o.job_id as created_bd_job_id

,'nal' as country_code
,'{pt}' as dt 
from
  (
  select
  a.id
  ,a.opay_id
  ,a.shop_name
  ,a.opay_account
  ,a.city_code
  ,a.city_name
  ,a.country
  ,a.job_id
  ,a.hcm_id
  ,a.hcm_name
  ,a.cm_id
  ,a.cm_name
  ,a.rm_id
  ,a.rm_name
  ,a.bdm_id
  ,a.bdm_name
  ,a.bd_id
  ,a.bd_name
  ,a.contact_name
  ,a.contact_phone
  ,a.cate_id
  ,a.status
  ,a.created_at
  ,a.shop_class
  ,nvl(b.first_order_date,'-') as first_order_date
  ,a.created_bd_id
  from
    (select * from opos_dw.opos_dw.dim_opos_bd_relation_tmp_df where country_code='nal' and dt='{pt}') as a
  left join
    (select id,first_order_date from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{before_1_day}') as b
  on a.id=b.id
  ) as m
left join
  (select receipt_id,'{pt}' as dt from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di where dt='{pt}' group by receipt_id) as n
on if(m.first_order_date='-',m.opay_id,'having')=n.receipt_id
left join
  (select id,name,job_id,phone from  opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as o
on m.created_bd_id=o.id
;




'''.format(
        pt=ds,
        before_1_day=airflow.macros.ds_add(ds, -1),
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct id) as cnt
      FROM opos_dw.dim_opos_bd_relation_df
      WHERE country_code='nal' and dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dim_opos_bd_relation_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dim_opos_bd_relation_df_task = PythonOperator(
    task_id='dim_opos_bd_relation_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_sqoop_base_bd_admin_users_df_task >> dim_opos_bd_relation_df_task
ods_sqoop_base_bd_shop_df_task >> dim_opos_bd_relation_df_task
ods_sqoop_base_bd_city_df_task >> dim_opos_bd_relation_df_task


