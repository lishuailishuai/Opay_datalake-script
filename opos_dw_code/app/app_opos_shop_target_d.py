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

dag = airflow.DAG('app_opos_shop_target_d',
                  schedule_interval="30 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

dwd_pre_opos_payment_order_di_task = UFileSensor(
    task_id='dwd_pre_opos_payment_order_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_pre_opos_payment_order_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_opos_shop_target_d"
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

def app_opos_shop_target_d_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--得出最新维度下每个dbid的详细数据信息
insert overwrite table opos_dw.app_opos_shop_target_d partition (country_code,dt)
select
a.shop_id
,a.opay_id
,a.shop_name
,a.opay_account

,'{pt}' as create_date
,d.week_of_year as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year

,a.city_code
,a.city_name
,a.country

,a.hcm_id
,nvl(hcm.name,'-') as hcm_name
,a.cm_id
,nvl(cm.name,'-') as cm_name
,a.rm_id
,nvl(rm.name,'-') as rm_name
,a.bdm_id
,nvl(bdm.name,'-') as bdm_name
,a.bd_id
,nvl(bd.name,'-') as bd_name

,nvl(b.shop_class,'-') as shop_class
,nvl(b.first_order_date,'-')  as shop_first_order_date

,a.order_cnt
,a.cashback_order_cnt
,a.cashback_fail_order_cnt
,a.cashback_order_gmv
,a.cashback_per_order_amt
,a.cashback_per_people_amt
,a.cashback_people_cnt
,a.cashback_first_people_cnt
,a.cashback_zero_order_cnt
,a.cashback_order_percent
,a.cashback_amt
,a.reduce_order_cnt
,a.reduce_zero_order_cnt
,a.reduce_amt
,a.reduce_order_gmv
,a.reduce_per_order_amt
,a.reduce_per_people_amt
,a.reduce_people_cnt
,a.reduce_first_people_cnt

,'nal' as country_code
,'{pt}' as dt

from
  (
  select 
  nvl(s.shop_id,m.shop_id) as shop_id
  ,nvl(s.opay_id,m.opay_id) as opay_id
  ,nvl(s.shop_name,m.shop_name) as shop_name
  ,nvl(s.opay_account,m.opay_account) as opay_account
  
  ,nvl(s.city_id,m.city_id) as city_code
  ,nvl(s.city_name,m.city_name) as city_name
  ,nvl(s.country,m.country) as country
  
  ,nvl(s.hcm_id,m.hcm_id) as hcm_id
  ,nvl(s.cm_id,m.cm_id) as cm_id
  ,nvl(s.rm_id,m.rm_id) as rm_id
  ,nvl(s.bdm_id,m.bdm_id) as bdm_id
  ,nvl(s.bd_id,m.bd_id) as bd_id
  
  ,nvl(order_cnt,0) as order_cnt
  ,nvl(cashback_order_cnt,0) as cashback_order_cnt
  ,nvl(cashback_fail_order_cnt,0) as cashback_fail_order_cnt
  ,nvl(cashback_order_gmv,0) as cashback_order_gmv
  ,nvl(cashback_per_order_amt,0) as cashback_per_order_amt
  ,nvl(cashback_per_people_amt,0) as cashback_per_people_amt
  ,nvl(cashback_people_cnt,0) as cashback_people_cnt
  ,nvl(cashback_first_people_cnt,0) as cashback_first_people_cnt
  ,nvl(cashback_zero_order_cnt,0) as cashback_zero_order_cnt
  ,nvl(cashback_order_percent,0) as cashback_order_percent
  ,nvl(cashback_amt,0) as cashback_amt
  
  ,nvl(reduce_order_cnt,0) as reduce_order_cnt
  ,nvl(reduce_zero_order_cnt,0) as reduce_zero_order_cnt
  ,nvl(reduce_amt,0) as reduce_amt
  ,nvl(reduce_order_gmv,0) as reduce_order_gmv
  ,nvl(reduce_per_order_amt,0) as reduce_per_order_amt
  ,nvl(reduce_per_people_amt,0) as reduce_per_people_amt
  ,nvl(reduce_people_cnt,0) as reduce_people_cnt
  ,nvl(reduce_first_people_cnt,0) as reduce_first_people_cnt
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
  
    --返现活动情况分析
    ,count(1) as order_cnt
    ,count(if(user_subsidy_status='SUCCESS',1,null)) as cashback_order_cnt
    ,count(if(user_subsidy_status!='SUCCESS',1,null)) as cashback_fail_order_cnt
    ,sum(if(user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0)) as cashback_order_gmv
    ,nvl(sum(if(user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0))/count(if(user_subsidy_status='SUCCESS',1,null)),0) as cashback_per_order_amt
    ,nvl(sum(if(user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0))/count(distinct(if(user_subsidy_status='SUCCESS',sender_id,null))),0) as cashback_per_people_amt
    ,count(distinct(if(user_subsidy_status='SUCCESS',sender_id,null))) as cashback_people_cnt
    ,count(distinct(if(user_subsidy_status='SUCCESS' and first_order='1',sender_id,null))) as cashback_first_people_cnt
    ,count(if(user_subsidy=0,1,null)) as cashback_zero_order_cnt
    ,nvl(count(if(user_subsidy_status='SUCCESS',1,null))/count(1),0) as cashback_order_percent
    ,sum(if(user_subsidy>0,nvl(user_subsidy,0),0)) as cashback_amt
  
    ,count(if(activity_type in ('RCB','CB'),1,null)) as reduce_order_cnt
    ,count(if(activity_type not in ('RCB','CB'),1,null)) as reduce_zero_order_cnt
    ,sum(if(activity_type not in ('RCB','CB'),nvl(discount_amount,0),0)) as reduce_amt
    ,sum(if(activity_type not in ('RCB','CB'),nvl(org_payment_amount,0),0)) as reduce_order_gmv
    ,nvl(sum(if(activity_type not in ('RCB','CB'),nvl(org_payment_amount,0),0))/count(if(activity_type in ('RCB','CB'),1,null)),0) as reduce_per_order_amt
    ,nvl(sum(if(activity_type not in ('RCB','CB'),nvl(org_payment_amount,0),0))/count(distinct(if(activity_type not in ('RCB','CB'),sender_id,null))),0) as reduce_per_people_amt
    ,count(distinct(if(activity_type not in ('RCB','CB'),sender_id,null))) as reduce_people_cnt
    ,count(distinct(if(activity_type not in ('RCB','CB') and first_order='1',sender_id,null))) as reduce_first_people_cnt
  
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
    ) as m
  on
    s.shop_id=m.shop_id
    and s.opay_id=m.opay_id
    and s.hcm_id=m.hcm_id
    and s.cm_id=m.cm_id
    and s.rm_id=m.rm_id
    and s.bdm_id=m.bdm_id
    and s.bd_id=m.bd_id
  ) as a
left join
  (select opay_id,first_order_date,shop_class from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as b
on a.opay_id=b.opay_id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bd
on a.bd_id=bd.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bdm
on a.bdm_id=bdm.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as rm
on a.rm_id=rm.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as cm
on a.cm_id=cm.id
left join
  (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as hcm
on a.hcm_id=hcm.id
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
    _sql = app_opos_shop_target_d_sql_task(ds)

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


app_opos_shop_target_d_task = PythonOperator(
    task_id='app_opos_shop_target_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_pre_opos_payment_order_di_task >> app_opos_shop_target_d_task

# 查看任务命令
# airflow list_tasks app_opos_shop_target_d -sd /home/feng.yuan/app_opos_shop_target_d.py
# 测试任务命令
# airflow test app_opos_shop_target_d app_opos_shop_target_d_task 2019-11-24 -sd /home/feng.yuan/app_opos_shop_target_d.py

