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

dag = airflow.DAG('app_opos_shop_target_d',
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

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
table_name = "app_opos_shop_target_d"
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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

def app_opos_shop_target_d_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--01.取出交易成功的商铺信息数据
with
payment_order as (
  select
  receipt_id as opay_id
  
  ,city_id_shop as city_id
  
  ,hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  --返现活动情况分析
  ,count(if(trade_status='SUCCESS',1,null)) as order_cnt
  ,count(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',1,null)) as cashback_order_cnt
  ,count(if(trade_status='SUCCESS' and user_subsidy_status='FAIL',1,null)) as cashback_fail_order_cnt
  ,sum(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0)) as cashback_order_gmv
  ,nvl(sum(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0))/count(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',1,null)),0) as cashback_per_order_amt
  ,nvl(sum(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0))/count(distinct(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',sender_id,null))),0) as cashback_per_people_amt
  ,count(distinct(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',sender_id,null))) as cashback_people_cnt
  ,count(distinct(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS' and first_order='1',sender_id,null))) as cashback_first_people_cnt
  ,count(if(trade_status='SUCCESS' and user_subsidy=0,1,null)) as cashback_zero_order_cnt
  ,nvl(count(if(trade_status='SUCCESS' and user_subsidy_status='SUCCESS',1,null))/count(1),0) as cashback_order_percent
  ,sum(if(trade_status='SUCCESS' and user_subsidy>0 and user_subsidy_status='SUCCESS',nvl(user_subsidy,0),0)) as cashback_amt
  
  ,count(if(trade_status='SUCCESS' and activity_type in ('RFR','FR'),1,null)) as reduce_order_cnt
  ,count(if(trade_status='SUCCESS' and activity_type not in ('RFR','FR'),1,null)) as reduce_zero_order_cnt
  ,sum(if(trade_status='SUCCESS' and activity_type in ('RFR','FR'),nvl(discount_amount,0),0)) as reduce_amt
  ,sum(if(trade_status='SUCCESS' and activity_type in ('RFR','FR'),nvl(org_payment_amount,0),0)) as reduce_order_gmv
  ,nvl(sum(if(trade_status='SUCCESS' and activity_type in ('RFR','FR'),nvl(org_payment_amount,0),0))/count(if(activity_type in ('RFR','FR'),1,null)),0) as reduce_per_order_amt
  ,nvl(sum(if(trade_status='SUCCESS' and activity_type in ('RFR','FR'),nvl(org_payment_amount,0),0))/count(distinct(if(activity_type not in ('RFR','FR'),sender_id,null))),0) as reduce_per_people_amt
  ,count(distinct(if(trade_status='SUCCESS' and activity_type in ('RFR','FR'),sender_id,null))) as reduce_people_cnt
  ,count(distinct(if(trade_status='SUCCESS' and activity_type in ('RFR','FR') and first_order='1',sender_id,null))) as reduce_first_people_cnt
  
  --使用红包起情况
  ,count(if(trade_status='SUCCESS' and length(discount_ids)>0,1,null)) as bonus_order_cnt
  
  --用户数量板块
  ,count(distinct(if(trade_status='SUCCESS',sender_id,null))) as order_people
  ,count(distinct(if(trade_status='SUCCESS' and first_order='0',sender_id,null))) as not_first_order_people
  ,count(distinct(if(trade_status='SUCCESS' and first_order='1',sender_id,null))) as first_order_people
  ,count(distinct(if(trade_status='SUCCESS' and length(discount_ids)>0 and first_order='1',sender_id,null))) as first_bonus_order_people
  
  --gmv板块
  ,sum(if(trade_status='SUCCESS',nvl(org_payment_amount,0),null)) as order_gmv
  ,sum(if(trade_status='SUCCESS' and length(discount_ids)>0,nvl(org_payment_amount,0),0)) as bonus_order_gmv
  
  --用户角度
  ,count(distinct(if(trade_status='SUCCESS' and length(discount_ids)>0,sender_id,null))) as bonus_order_people
  ,count(if(trade_status='SUCCESS' and length(discount_ids)>0,1,null)) as bonus_order_times
  
  --与红包无关的指标
  ,nvl(sum(if(trade_status='SUCCESS',nvl(org_payment_amount,0),null))/count(if(trade_status='SUCCESS',1,null)),0) as order_avg_amt
  ,nvl(sum(if(trade_status='SUCCESS',nvl(org_payment_amount,0),null))/count(distinct(if(trade_status='SUCCESS',sender_id,null))),0) as people_avg_amt

  --订单漏斗相关指标
  ,count(1) as order_create_cnt
  ,count(if(trade_status in ('SUCCESS','FAIL','PENDING'),1,null)) as order_pay_cnt
  ,count(if(trade_status = 'FAIL',1,null)) as order_fail_cnt
  ,count(if(trade_status = 'PENDING',1,null)) as order_pending_cnt
  
  from
  opos_dw.dwd_pre_opos_payment_order_di 
  where country_code='nal' 
  and dt='{pt}' 
  group BY
  receipt_id
  ,city_id_shop
  
  ,hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
),

--02.再求出红包最新的交易数据
bonus_order as (
  select
  provider_opay_id as opay_id
  ,provider_city_id as city_id
  
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
  provider_opay_id
  ,provider_city_id
  
  ,hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
),

--03.然后将交易数据和红包数据做全关联,取出并集
payment_bonus_order as (
  select
  nvl(p.opay_id,bo.opay_id) as opay_id
  
  ,nvl(p.city_id,bo.city_id) as city_id
  
  ,nvl(p.hcm_id,bo.hcm_id) as hcm_id
  ,nvl(p.cm_id,bo.cm_id) as cm_id
  ,nvl(p.rm_id,bo.rm_id) as rm_id
  ,nvl(p.bdm_id,bo.bdm_id) as bdm_id
  ,nvl(p.bd_id,bo.bd_id) as bd_id

  ,nvl(p.order_cnt,0) as order_cnt
  ,nvl(p.cashback_order_cnt,0) as cashback_order_cnt
  ,nvl(p.cashback_fail_order_cnt,0) as cashback_fail_order_cnt
  ,nvl(p.cashback_order_gmv,0) as cashback_order_gmv
  ,nvl(p.cashback_per_order_amt,0) as cashback_per_order_amt
  ,nvl(p.cashback_per_people_amt,0) as cashback_per_people_amt
  ,nvl(p.cashback_people_cnt,0) as cashback_people_cnt
  ,nvl(p.cashback_first_people_cnt,0) as cashback_first_people_cnt
  ,nvl(p.cashback_zero_order_cnt,0) as cashback_zero_order_cnt
  ,nvl(p.cashback_order_percent,0) as cashback_order_percent
  ,nvl(p.cashback_amt,0) as cashback_amt
  
  ,nvl(p.reduce_order_cnt,0) as reduce_order_cnt
  ,nvl(p.reduce_zero_order_cnt,0) as reduce_zero_order_cnt
  ,nvl(p.reduce_amt,0) as reduce_amt
  ,nvl(p.reduce_order_gmv,0) as reduce_order_gmv
  ,nvl(p.reduce_per_order_amt,0) as reduce_per_order_amt
  ,nvl(p.reduce_per_people_amt,0) as reduce_per_people_amt
  ,nvl(p.reduce_people_cnt,0) as reduce_people_cnt
  ,nvl(p.reduce_first_people_cnt,0) as reduce_first_people_cnt
  
  ,nvl(p.bonus_order_cnt,0) as bonus_order_cnt
  ,nvl(p.order_people,0) as order_people
  ,nvl(p.not_first_order_people,0) as not_first_order_people
  ,nvl(p.first_order_people,0) as first_order_people
  ,nvl(p.first_bonus_order_people,0) as first_bonus_order_people
  ,nvl(p.order_gmv,0) as order_gmv
  ,nvl(p.bonus_order_gmv,0) as bonus_order_gmv
  
  ,nvl(bo.bonus_order_amt,0) as bonus_order_amt
  ,nvl(bo.sweep_amt,0) as sweep_amt
  ,nvl(bo.bonus_use_percent,0) as bonus_use_percent
  
  ,nvl(p.bonus_order_people,0) as bonus_order_people
  ,nvl(p.bonus_order_times,0) as bonus_order_times
  ,nvl(p.order_avg_amt,0) as order_avg_amt
  ,nvl(p.people_avg_amt,0) as people_avg_amt

  ,nvl(p.order_create_cnt,0) as order_create_cnt
  ,nvl(p.order_pay_cnt,0) as order_pay_cnt
  ,nvl(p.order_fail_cnt,0) as order_fail_cnt
  ,nvl(p.order_pending_cnt,0) as order_pending_cnt

  from
  payment_order as p
  full join
  bonus_order as bo
  on
  p.opay_id=bo.opay_id
  
  and p.city_id=bo.city_id
  
  and p.hcm_id=bo.hcm_id
  and p.cm_id=bo.cm_id
  and p.rm_id=bo.rm_id
  and p.bdm_id=bo.bdm_id
  and p.bd_id=bo.bd_id
),

--04.根据商铺主数据表求出所有商铺
all_shop as (
  select
  opay_id
  ,city_code as city_id
  
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
  opay_id
  ,city_code
  
  ,hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
),

--05.然后将全量商铺数据和交易红包数据做关联,求出最后的数据并集
shop_payment_bonus as (
  select 
  nvl(s.opay_id,m.opay_id) as opay_id
  ,nvl(s.city_id,m.city_id) as city_code
  
  ,nvl(s.hcm_id,m.hcm_id) as hcm_id
  ,nvl(s.cm_id,m.cm_id) as cm_id
  ,nvl(s.rm_id,m.rm_id) as rm_id
  ,nvl(s.bdm_id,m.bdm_id) as bdm_id
  ,nvl(s.bd_id,m.bd_id) as bd_id
  
  ,nvl(m.order_cnt,0) as order_cnt
  ,nvl(m.cashback_order_cnt,0) as cashback_order_cnt
  ,nvl(m.cashback_fail_order_cnt,0) as cashback_fail_order_cnt
  ,nvl(m.cashback_order_gmv,0) as cashback_order_gmv
  ,nvl(m.cashback_per_order_amt,0) as cashback_per_order_amt
  ,nvl(m.cashback_per_people_amt,0) as cashback_per_people_amt
  ,nvl(m.cashback_people_cnt,0) as cashback_people_cnt
  ,nvl(m.cashback_first_people_cnt,0) as cashback_first_people_cnt
  ,nvl(m.cashback_zero_order_cnt,0) as cashback_zero_order_cnt
  ,nvl(m.cashback_order_percent,0) as cashback_order_percent
  ,nvl(m.cashback_amt,0) as cashback_amt
  
  ,nvl(m.reduce_order_cnt,0) as reduce_order_cnt
  ,nvl(m.reduce_zero_order_cnt,0) as reduce_zero_order_cnt
  ,nvl(m.reduce_amt,0) as reduce_amt
  ,nvl(m.reduce_order_gmv,0) as reduce_order_gmv
  ,nvl(m.reduce_per_order_amt,0) as reduce_per_order_amt
  ,nvl(m.reduce_per_people_amt,0) as reduce_per_people_amt
  ,nvl(m.reduce_people_cnt,0) as reduce_people_cnt
  ,nvl(m.reduce_first_people_cnt,0) as reduce_first_people_cnt

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

  ,nvl(m.order_create_cnt,0) as order_create_cnt
  ,nvl(m.order_pay_cnt,0) as order_pay_cnt
  ,nvl(m.order_fail_cnt,0) as order_fail_cnt
  ,nvl(m.order_pending_cnt,0) as order_pending_cnt

  from
    --商铺全量数据
    all_shop as s
  full join
    --交易和红包全量数据
    payment_bonus_order as m
  on
    s.opay_id=m.opay_id
    and s.hcm_id=m.hcm_id
    and s.cm_id=m.cm_id
    and s.rm_id=m.rm_id
    and s.bdm_id=m.bdm_id
    and s.bd_id=m.bd_id
)

--得出最新维度下每个dbid的详细数据信息
insert overwrite table opos_dw.app_opos_shop_target_d partition (country_code,dt)
select
0 as id
,nvl(b.id,'-') as shop_id
,a.opay_id
,nvl(b.shop_name,'-') as shop_name
,nvl(b.opay_account,'-') as opay_account

,substr('{pt}',0,10) as create_date
,weekofyear('{pt}') as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year

,a.city_code
,nvl(c.name,'-') as city_name
,nvl(c.country,'-') as country

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
,nvl(b.created_at,'-') as created_at
,nvl(b.created_bd_id,0) as created_bd_id
,nvl(b.created_bd_phone,'-') as created_bd_phone
,nvl(b.created_bd_name,'-') as created_bd_name
,nvl(b.created_bd_job_id,'-') as created_bd_job_id

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

,a.bonus_order_cnt
,a.order_people
,a.not_first_order_people
,a.first_order_people
,a.first_bonus_order_people
,a.order_gmv
,a.bonus_order_gmv
,a.bonus_order_amt
,a.sweep_amt
,a.bonus_use_percent
,a.bonus_order_people
,a.bonus_order_times
,a.order_avg_amt
,a.people_avg_amt

,a.order_create_cnt
,a.order_pay_cnt
,a.order_fail_cnt
,a.order_pending_cnt

,nvl(b.status,-1) as status

,'nal' as country_code
,'{pt}' as dt

from
  shop_payment_bonus as a
left join
  (select * from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as b
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
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as c
on a.city_code=c.id
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

