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
    'start_date': datetime(2019, 11, 29),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_bonus_shop_target_d',
                  schedule_interval="10 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_opos_bonus_record_di_task = OssSensor(
    task_id='dwd_opos_bonus_record_di_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_opos_bonus_record_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_pre_opos_payment_order_di_task = OssSensor(
    task_id='dwd_pre_opos_payment_order_di_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_pre_opos_payment_order_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_opos_bonus_shop_target_d"
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


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

def app_opos_bonus_shop_target_d_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--02.用shop表计算出每个bd下有
--得出最新维度下每个dbid的详细数据信息
insert overwrite table opos_dw.app_opos_bonus_shop_target_d partition (country_code,dt)
select
o.shop_id
,o.opay_id
,o.shop_name
,o.opay_account

,'{pt}' as create_date
,d.week_of_year as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year

,o.city_id as city_code
,o.city_name
,o.country

,o.hcm_id
,nvl(hcm.name,'-') as hcm_name
,o.cm_id
,nvl(cm.name,'-') as cm_name
,o.rm_id
,nvl(rm.name,'-') as rm_name
,o.bdm_id
,nvl(bdm.name,'-') as bdm_name
,o.bd_id
,nvl(bd.name,'-') as bd_name

,o.order_cnt
,o.bonus_order_cnt
,o.order_people
,o.not_first_order_people
,o.first_order_people
,o.first_bonus_order_people
,o.order_gmv
,o.bonus_order_gmv
,o.bonus_order_amt
,o.sweep_amt
,o.bonus_use_percent
,o.bonus_order_people
,o.bonus_order_times
,o.order_avg_amt
,o.people_avg_amt

,'nal' as country_code
,'{pt}' as dt
from
  (
  select
  nvl(s.shop_id,m.shop_id) as shop_id
  ,nvl(s.opay_id,m.opay_id) as opay_id
  ,nvl(s.shop_name,m.shop_name) as shop_name
  ,nvl(s.opay_account,m.opay_account) as opay_account
  
  ,nvl(s.city_id,m.city_id) as city_id
  ,nvl(s.city_name,m.city_name) as city_name
  ,nvl(s.country,m.country) as country
  
  ,nvl(s.hcm_id,m.hcm_id) as hcm_id
  ,nvl(s.cm_id,m.cm_id) as cm_id
  ,nvl(s.rm_id,m.rm_id) as rm_id
  ,nvl(s.bdm_id,m.bdm_id) as bdm_id
  ,nvl(s.bd_id,m.bd_id) as bd_id
  
  ,nvl(m.order_cnt,0) as order_cnt
  ,nvl(m.bonus_order_cnt,0) as bonus_order_cnt
  ,nvl(m.order_people,0) as order_people
  ,nvl(m.not_first_order_people,0) as not_first_order_people
  ,nvl(m.first_order_people,0) as first_order_people
  ,nvl(m.first_bonus_order_people,0) as first_bonus_order_people
  ,nvl(m.order_gmv,0) as order_gmv
  ,nvl(m.bonus_order_gmv,0) as bonus_order_gmv
  
  ,nvl(m.bonus_order_amt,0) as bonus_order_amt
  ,nvl(m.sweep_amt,0) as sweep_amt
  ,nvl(m.bonus_use_percent,0) as bonus_use_percent
  
  ,nvl(m.bonus_order_people,0) as bonus_order_people
  ,nvl(m.bonus_order_times,0) as bonus_order_times
  ,nvl(m.order_avg_amt,0) as order_avg_amt
  ,nvl(m.people_avg_amt,0) as people_avg_amt
  from
    (
    select
    id as shop_id
    ,opay_id
    ,shop_name
    ,opay_account
  
    ,city_code as city_id
    ,city_name
    ,country
  
    ,hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
    FROM 
    opos_dw.dim_opos_bd_relation_df 
    where 
    country_code='nal' 
    and dt='{pt}'
    group by 
    id
    ,opay_id
    ,shop_name
    ,opay_account
  
    ,city_code
    ,city_name
    ,country
  
    ,hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
  
    ) as s
  full join
    (
    select
    nvl(a.shop_id,b.shop_id) as shop_id
    ,nvl(a.opay_id,b.opay_id) as opay_id
    ,nvl(a.shop_name,b.shop_name) as shop_name
    ,nvl(a.opay_account,b.opay_account) as opay_account
    
    ,nvl(a.city_id,b.city_id) as city_id
    ,nvl(a.city_name,b.city_name) as city_name
    ,nvl(a.country,b.country) as country
    
    ,nvl(a.hcm_id,b.hcm_id) as hcm_id
    ,nvl(a.cm_id,b.cm_id) as cm_id
    ,nvl(a.rm_id,b.rm_id) as rm_id
    ,nvl(a.bdm_id,b.bdm_id) as bdm_id
    ,nvl(a.bd_id,b.bd_id) as bd_id
    
    ,nvl(a.order_cnt,0) as order_cnt
    ,nvl(a.bonus_order_cnt,0) as bonus_order_cnt
    ,nvl(a.order_people,0) as order_people
    ,nvl(a.not_first_order_people,0) as not_first_order_people
    ,nvl(a.first_order_people,0) as first_order_people
    ,nvl(a.first_bonus_order_people,0) as first_bonus_order_people
    ,nvl(a.order_gmv,0) as order_gmv
    ,nvl(a.bonus_order_gmv,0) as bonus_order_gmv
    
    ,nvl(b.bonus_order_amt,0) as bonus_order_amt
    ,nvl(b.sweep_amt,0) as sweep_amt
    ,nvl(b.bonus_use_percent,0) as bonus_use_percent
    
    ,nvl(a.bonus_order_people,0) as bonus_order_people
    ,nvl(a.bonus_order_times,0) as bonus_order_times
    ,nvl(a.order_avg_amt,0) as order_avg_amt
    ,nvl(a.people_avg_amt,0) as people_avg_amt
    
    from
      (
      select
      shop_id
      ,receipt_id as opay_id
      ,shop_name
      ,opay_account
      
      ,city_id_shop as city_id
      ,city_name_shop as city_name
      ,country_shop as country
      
      ,hcm_id
      ,cm_id
      ,rm_id
      ,bdm_id
      ,bd_id
      
      --使用红包起情况
      ,count(1) as order_cnt
      ,count(if(length(discount_ids)>0,1,null)) as bonus_order_cnt
      
      --用户数量板块
      ,count(distinct(sender_id)) as order_people
      ,count(distinct(if(first_order='0',sender_id,null))) as not_first_order_people
      ,count(distinct(if(first_order='1',sender_id,null))) as first_order_people
      ,count(distinct(if(length(discount_ids)>0 and first_order='1',sender_id,null))) as first_bonus_order_people
      
      --gmv板块
      ,sum(nvl(org_payment_amount,0)) as order_gmv
      ,sum(if(length(discount_ids)>0,nvl(org_payment_amount,0),0)) as bonus_order_gmv
      
      --用户角度
      ,count(distinct(if(length(discount_ids)>0,sender_id,null))) as bonus_order_people
      ,count(if(length(discount_ids)>0,1,null)) as bonus_order_times
      
      --与红包无关的指标
      ,nvl(sum(nvl(org_payment_amount,0))/count(1),0) as order_avg_amt
      ,nvl(sum(nvl(org_payment_amount,0))/count(distinct(sender_id)),0) as people_avg_amt
      from
      opos_dw.dwd_pre_opos_payment_order_di 
      where country_code='nal' 
      and dt='{pt}' 
      and trade_status='SUCCESS'
      group BY
      shop_id
      ,receipt_id
      ,shop_name
      ,opay_account
      
      ,city_id_shop
      ,city_name_shop
      ,country_shop
      
      ,hcm_id
      ,cm_id
      ,rm_id
      ,bdm_id
      ,bd_id
    
      ) as a
      full join
      (
      select
      provider_shop_id as shop_id
      ,provider_opay_id as opay_id
      ,provider_shop_name as shop_name
      ,provider_account as opay_account
      
      ,provider_city_id as city_id
      ,provider_city_name as city_name
      ,provider_country as country
      
      ,hcm_id
      ,cm_id
      ,rm_id
      ,bdm_id
      ,bd_id
    
      ,sum(use_amount) as bonus_order_amt
      ,sum(bonus_amount) as sweep_amt
      ,nvl(sum(use_amount)/sum(bonus_amount),0) as bonus_use_percent
      from
      opos_dw.dwd_opos_bonus_record_di 
      where 
      country_code='nal' 
      and dt='{pt}'
      and provider_shop_id is not null
      group BY
      provider_shop_id
      ,provider_opay_id
      ,provider_shop_name
      ,provider_account
      
      ,provider_city_id
      ,provider_city_name
      ,provider_country
      
      ,hcm_id
      ,cm_id
      ,rm_id
      ,bdm_id
      ,bd_id
      ) as b
      on
      a.shop_id=b.shop_id
      and a.opay_id=b.opay_id
      and a.shop_name=b.shop_name
      and a.opay_account=b.opay_account
    
      and a.city_id=b.city_id
      and a.city_name=b.city_name
      and a.country=b.country
    
      and a.hcm_id=b.hcm_id
      and a.cm_id=b.cm_id
      and a.rm_id=b.rm_id
      and a.bdm_id=b.bdm_id
      and a.bd_id=b.bd_id
    ) as m
    on
    s.shop_id=m.shop_id
    and s.opay_id=m.opay_id
    and s.shop_name=m.shop_name
    and s.opay_account=m.opay_account
    
    and s.city_id=m.city_id
    and s.city_name=m.city_name
    and s.country=m.country
    
    and s.hcm_id=m.hcm_id
    and s.cm_id=m.cm_id
    and s.rm_id=m.rm_id
    and s.bdm_id=m.bdm_id
    and s.bd_id=m.bd_id
  ) as o
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bd
  on o.bd_id=bd.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bdm
on o.bdm_id=bdm.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as rm
on o.rm_id=rm.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as cm
on o.cm_id=cm.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as hcm
  on o.hcm_id=hcm.id
left join
  (select dt,week_of_year from public_dw_dim.dim_date where dt = '{pt}') as d
on 1=1
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
    _sql = app_opos_bonus_shop_target_d_sql_task(ds)

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


app_opos_bonus_shop_target_d_task = PythonOperator(
    task_id='app_opos_bonus_shop_target_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opos_bonus_record_di_task >> app_opos_bonus_shop_target_d_task
dwd_pre_opos_payment_order_di_task >> app_opos_bonus_shop_target_d_task

# 查看任务命令
# airflow list_tasks app_opos_bonus_shop_target_d -sd /home/feng.yuan/app_opos_bonus_shop_target_d.py
# 测试任务命令
# airflow test app_opos_bonus_shop_target_d app_opos_bonus_shop_target_d_task 2019-11-24 -sd /home/feng.yuan/app_opos_bonus_shop_target_d.py

