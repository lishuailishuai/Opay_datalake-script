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

dag = airflow.DAG('app_opos_metrics_daily_new_d',
                  schedule_interval="20 04 * * *",
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


##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_opos_metrics_daily_new_d"
hdfs_path = "oss://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "6000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_opos_metrics_daily_new_d_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--01.向report临时表中插入当日销售和订单的数据
insert overwrite table opos_dw.app_opos_report_mid partition (country_code,dt)
select
nvl(a.hcm_id,b.hcm_id) as hcm_id
,nvl(a.cm_id,b.cm_id) as cm_id
,nvl(a.rm_id,b.rm_id) as rm_id
,nvl(a.bdm_id,b.bdm_id) as bdm_id
,nvl(a.bd_id,b.bd_id) as bd_id

,nvl(a.city_id,b.city_id) as city_id

,nvl(a.merchant_cnt,0) as merchant_cnt
,nvl(a.pos_merchant_cnt,0) as pos_merchant_cnt
,nvl(a.new_merchant_cnt,0) as new_merchant_cnt
,nvl(a.new_pos_merchant_cnt,0) as new_pos_merchant_cnt

,nvl(b.pos_complete_order_cnt,0) as pos_complete_order_cnt
,nvl(b.qr_complete_order_cnt,0) as qr_complete_order_cnt
,nvl(b.complete_order_cnt,0) as complete_order_cnt
,nvl(b.gmv,0) as gmv
,nvl(b.actual_amount,0) as actual_amount
,nvl(b.return_amount,0) as return_amount
,nvl(b.new_user_cost,0) as new_user_cost
,nvl(b.old_user_cost,0) as old_user_cost
,nvl(b.return_amount_order_cnt,0) as return_amount_order_cnt

,nvl(b.cashback_order_cnt,0) as cashback_order_cnt
,nvl(b.cashback_fail_order_cnt,0) as cashback_fail_order_cnt
,nvl(b.cashback_order_gmv,0) as cashback_order_gmv
,nvl(b.cashback_per_order_amt,0) as cashback_per_order_amt
,nvl(b.cashback_per_people_amt,0) as cashback_per_people_amt
,nvl(b.cashback_people_cnt,0) as cashback_people_cnt
,nvl(b.cashback_first_people_cnt,0) as cashback_first_people_cnt
,nvl(b.cashback_zero_order_cnt,0) as cashback_zero_order_cnt
,nvl(b.cashback_order_percent,0) as cashback_order_percent
,nvl(b.cashback_amt,0) as cashback_amt

,nvl(b.reduce_order_cnt,0) as reduce_order_cnt
,nvl(b.reduce_zero_order_cnt,0) as reduce_zero_order_cnt
,nvl(b.reduce_amt,0) as reduce_amt
,nvl(b.reduce_order_gmv,0) as reduce_order_gmv
,nvl(b.reduce_per_order_amt,0) as reduce_per_order_amt
,nvl(b.reduce_per_people_amt,0) as reduce_per_people_amt
,nvl(b.reduce_people_cnt,0) as reduce_people_cnt
,nvl(b.reduce_first_people_cnt,0) as reduce_first_people_cnt

,'nal' as country_code
,'{pt}' as dt
from
  (select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_code as city_id
  
  ,count(id) as merchant_cnt
  ,0 as pos_merchant_cnt
  ,count(if(created_at = '{pt}',id,null)) as new_merchant_cnt
  ,0 as new_pos_merchant_cnt
  from
  opos_dw.dim_opos_bd_relation_df
  where 
  country_code='nal' and dt='{pt}'
  group by
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_code
  ) as a
full join
  (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_id
  
  ,nvl(count(if(order_type = 'pos',order_id,null)),0) as pos_complete_order_cnt
  ,nvl(count(if(order_type = 'qrcode',order_id,null)),0) as qr_complete_order_cnt
  ,nvl(count(order_id),0) as complete_order_cnt
  ,nvl(sum(nvl(org_payment_amount,0)),0) as gmv
  ,nvl(sum(nvl(pay_amount,0)),0) as actual_amount
  ,nvl(sum(nvl(return_amount,0)),0) as return_amount
  ,nvl(sum(if(first_order = '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0),0)),0) as new_user_cost
  ,nvl(sum(if(first_order <> '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0),0)),0) as old_user_cost
  ,nvl(count(if(return_amount > 0,order_id,null)),0) as return_amount_order_cnt
  
  --返现活动情况分析
  ,count(1) as order_cnt
  ,count(if(user_subsidy_status='SUCCESS',1,null)) as cashback_order_cnt
  ,count(if(user_subsidy_status='FAIL',1,null)) as cashback_fail_order_cnt
  ,sum(if(user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0)) as cashback_order_gmv
  ,nvl(sum(if(user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0))/count(if(user_subsidy_status='SUCCESS',1,null)),0) as cashback_per_order_amt
  ,nvl(sum(if(user_subsidy_status='SUCCESS',nvl(org_payment_amount,0),0))/count(distinct(if(user_subsidy_status='SUCCESS',sender_id,null))),0) as cashback_per_people_amt
  ,count(distinct(if(user_subsidy_status='SUCCESS',sender_id,null))) as cashback_people_cnt
  ,count(distinct(if(user_subsidy_status='SUCCESS' and first_order='1',sender_id,null))) as cashback_first_people_cnt
  ,count(if(user_subsidy=0,1,null)) as cashback_zero_order_cnt
  ,nvl(count(if(user_subsidy_status='SUCCESS',1,null))/count(1),0) as cashback_order_percent
  ,sum(if(user_subsidy>0 and user_subsidy_status='SUCCESS',nvl(user_subsidy,0),0)) as cashback_amt
  
  ,count(if(activity_type in ('RFR','FR'),1,null)) as reduce_order_cnt
  ,count(if(activity_type not in ('RFR','FR'),1,null)) as reduce_zero_order_cnt
  ,sum(if(activity_type in ('RFR','FR'),nvl(discount_amount,0),0)) as reduce_amt
  ,sum(if(activity_type in ('RFR','FR'),nvl(org_payment_amount,0),0)) as reduce_order_gmv
  ,nvl(sum(if(activity_type in ('RFR','FR'),nvl(org_payment_amount,0),0))/count(if(activity_type in ('RFR','FR'),1,null)),0) as reduce_per_order_amt
  ,nvl(sum(if(activity_type in ('RFR','FR'),nvl(org_payment_amount,0),0))/count(distinct(if(activity_type not in ('RFR','FR'),sender_id,null))),0) as reduce_per_people_amt
  ,count(distinct(if(activity_type in ('RFR','FR'),sender_id,null))) as reduce_people_cnt
  ,count(distinct(if(activity_type in ('RFR','FR') and first_order='1',sender_id,null))) as reduce_first_people_cnt
  
  ,'nal' as country_code
  ,'{pt}' as dt
  
  from 
  opos_dw.dwd_pre_opos_payment_order_di as p
  where 
  country_code='nal'
  and dt = '{pt}' 
  and trade_status = 'SUCCESS'
  group by
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_id
  ) as b
on
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id
;



--02.插入当日客户数据,左表为本月数据情况
with
active_base as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  
  ,sender_id
  ,receipt_id
  ,order_type
  ,trade_status
  ,first_order

  ,dt
  from 
  opos_dw.dwd_pre_opos_payment_order_di
  where 
  country_code = 'nal' 
  and dt in ('{pt}','{before_1_day}','{before_7_day}','{before_15_day}','{before_30_day}')
  and trade_status = 'SUCCESS'
),

--03.02.然后从昨天,前天等等特殊历史天数中每个付款客户及对应付款店dbid的次数
user_base as ( 
  select  
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id

  ,sender_id
  ,order_type
  ,sum(if(dt = '{pt}',1,0)) as is_current_day
  ,sum(if(dt ='{before_1_day}',1,0)) as is_before_1_day
  ,sum(if(dt = '{before_7_day}',1,0)) as is_before_7_day
  ,sum(if(dt = '{before_15_day}',1,0)) as is_before_15_day
  ,sum(if(dt = '{before_30_day}',1,0)) as is_before_30_day
  ,sum(if(dt ='{pt}' and first_order = '1',1,0)) as is_new
  from 
  active_base
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id

  ,sender_id
  ,order_type
),

--03.03.统计昨日有交易的交易,然后统计每个dbid下的每种订单类型有多数个用户
current_user as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id

  ,count(distinct sender_id) as user_active_cnt
  ,count(distinct if(order_type = 'pos',sender_id,null)) as pos_user_active_cnt
  ,count(distinct if(order_type = 'qrcode',sender_id,null)) as qr_user_active_cnt
  ,count(distinct if(is_new > 0,sender_id,null)) as new_user_cnt

  from 
  user_base 
  where 
  is_current_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.04.统计近两天都有交易的客户对应到dbid下的人数
day_1_remain as (

  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_1_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.05.统计昨天和7天前都有交易的客户对应到dbid下的人数
day_7_remain as (

  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_7_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.06.统计昨天和15天前都有交易的客户
day_15_remain as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_15_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.07.统计昨天和30天前都有交易的客户
day_30_remain as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct sender_id) as user_active_cnt
  from 
  user_base 
  where is_current_day > 0 and is_before_30_day > 0
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.08.统计每个dbid下交易成功的商户数量以及用pos交易的商户数量
order_merchant_data as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct(receipt_id)) as order_merchant_cnt
  ,count(distinct(if(order_type = 'pos',receipt_id,null))) as pos_order_merchant_cnt
  from 
  active_base
  where dt = '{pt}'
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.09.join是inner join取维内自然周的数据
week_data as (
  select 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
  ,mt.monday_of_year
  ,count(distinct(if(u.order_type = 'pos',u.sender_id,null))) as pos_user_active_cnt
  ,count(distinct(if(u.order_type = 'qrcode',u.sender_id,null))) as qr_user_active_cnt
  from 
    (
    select 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
    ,city_id
    ,order_type
    ,sender_id
    ,dt
    from 
    opos_dw.dwd_pre_opos_payment_order_di 
    where 
    country_code = 'nal' 
    and dt <= '{pt}' 
    and dt >= '{before_7_day}'
    and trade_status = 'SUCCESS'
    ) u 
  inner join 
  --取出当日所在日期的本周的所有7天的日期
    (select d.dt,d.monday_of_year from public_dw_dim.dim_date d inner join (select * from public_dw_dim.dim_date where dt = '{pt}') t on d.monday_of_year = t.monday_of_year) as mt
  on u.dt = mt.dt
  group by 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
  ,mt.monday_of_year
),

--03.10.求出每月的dbid分别是pos和qrcode交易的笔数,只拿出所属月的
month_data as (
  select 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
  ,substr('{pt}',0,7) as month
  ,count(distinct(if(u.order_type = 'pos',u.sender_id,null))) as pos_user_active_cnt
  ,count(distinct(if(u.order_type = 'qrcode',u.sender_id,null))) as qr_user_active_cnt
  ,count(distinct(if(u.created_at>=concat(substr('{pt}',0,7),'-01') and u.created_at<='{pt}' and first_order='1',u.receipt_id,null))) as month_order_newshop_cnt
  from 
  opos_dw.dwd_pre_opos_payment_order_di as u
  where 
  country_code = 'nal' 
  and dt <= '{pt}' 
  and dt >= concat(substr('{pt}',0,7),'-01')
  and trade_status = 'SUCCESS'
  group by 
  u.hcm_id
  ,u.cm_id
  ,u.rm_id
  ,u.bdm_id
  ,u.bd_id

  ,u.city_id
),

--03.11.取出当天所有付款者的数量
have_order_user as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct(sender_id)) as have_order_user_cnt
  from 
  active_base
  where 
  dt = '{pt}'
  group by 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
),

--03.13.查出当天活跃的商户,然后计算成功交易次数大于5次的商户所属的bdid所对应的商户个数
active_merchant as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
  ,count(distinct(t.receipt_id)) as more_5_merchant_cnt

  from 
  --先计算每个商户成功交易的笔数
    (
    select 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
  
    ,city_id
    ,receipt_id
    ,count(1) as current_complete_cnt 
    from 
    opos_dw.dwd_pre_opos_payment_order_di 
    where 
    country_code = 'nal' 
    and dt = '{pt}' 
    and trade_status = 'SUCCESS'
    group by 
    hcm_id
    ,cm_id
    ,rm_id
    ,bdm_id
    ,bd_id
  
    ,city_id
    ,receipt_id) t 
  where 
  current_complete_cnt > 5
  group by
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id

  ,city_id
)

--最后将各个视图的数据插入到最终表中,其中,用本月二维码活跃用户作为最左表
insert overwrite table opos_dw.app_opos_active_user_daily_mid partition(country_code,dt)
select 
cu.hcm_id
,cu.cm_id
,cu.rm_id
,cu.bdm_id
,cu.bd_id

,cu.city_id

,substr('{pt}',0,10) as create_date
,weekofyear('{pt}') as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year

,nvl(hd.pos_user_active_cnt,0) pos_user_active_cnt
,nvl(hd.qr_user_active_cnt,0) qr_user_active_cnt
,nvl(dr1.user_active_cnt,0) before_1_day_user_active_cnt
,nvl(dr7.user_active_cnt,0) before_7_day_user_active_cnt
,nvl(dr15.user_active_cnt,0) before_15_day_user_active_cnt
,nvl(dr30.user_active_cnt,0) before_30_day_user_active_cnt
,nvl(omd.order_merchant_cnt,0) order_merchant_cnt
,nvl(omd.pos_order_merchant_cnt,0) pos_order_merchant_cnt
,nvl(wd.pos_user_active_cnt,0) week_pos_user_active_cnt
,nvl(wd.qr_user_active_cnt,0) week_qr_user_active_cnt
,nvl(cu.pos_user_active_cnt,0) month_pos_user_active_cnt
,nvl(cu.qr_user_active_cnt,0) month_qr_user_active_cnt
,nvl(ou.have_order_user_cnt,0) have_order_user_cnt

,nvl(hd.user_active_cnt,0) user_active_cnt
,nvl(hd.new_user_cnt,0) new_user_cnt
,nvl(am.more_5_merchant_cnt,0) more_5_merchant_cnt

,nvl(cu.month_order_newshop_cnt,0) month_order_newshop_cnt

,'nal' as country_code
,'{pt}' as dt

from 
month_data cu
left join 
day_1_remain dr1 
on cu.hcm_id = dr1.hcm_id and cu.cm_id = dr1.cm_id and cu.rm_id = dr1.rm_id and cu.bdm_id = dr1.bdm_id and cu.bd_id = dr1.bd_id and cu.city_id = dr1.city_id
left join 
day_7_remain dr7 
on cu.hcm_id = dr7.hcm_id and cu.cm_id = dr7.cm_id and cu.rm_id = dr7.rm_id and cu.bdm_id = dr7.bdm_id and cu.bd_id = dr7.bd_id and cu.city_id = dr7.city_id
left join 
day_15_remain dr15 
on cu.hcm_id = dr15.hcm_id and cu.cm_id = dr15.cm_id and cu.rm_id = dr15.rm_id and cu.bdm_id = dr15.bdm_id and cu.bd_id = dr15.bd_id and cu.city_id = dr15.city_id
left join 
day_30_remain dr30 
on cu.hcm_id = dr30.hcm_id and cu.cm_id = dr30.cm_id and cu.rm_id = dr30.rm_id and cu.bdm_id = dr30.bdm_id and cu.bd_id = dr30.bd_id and cu.city_id = dr30.city_id
left join 
order_merchant_data omd 
on cu.hcm_id = omd.hcm_id and cu.cm_id = omd.cm_id and cu.rm_id = omd.rm_id and cu.bdm_id = omd.bdm_id and cu.bd_id = omd.bd_id and cu.city_id = omd.city_id
left join 
week_data wd 
on cu.hcm_id = wd.hcm_id and cu.cm_id = wd.cm_id and cu.rm_id = wd.rm_id and cu.bdm_id = wd.bdm_id and cu.bd_id = wd.bd_id and cu.city_id = wd.city_id
left join 
have_order_user ou 
on cu.hcm_id = ou.hcm_id and cu.cm_id = ou.cm_id and cu.rm_id = ou.rm_id and cu.bdm_id = ou.bdm_id and cu.bd_id = ou.bd_id and cu.city_id = ou.city_id
left join 
current_user hd 
on cu.hcm_id = hd.hcm_id and cu.cm_id = hd.cm_id and cu.rm_id = hd.rm_id and cu.bdm_id = hd.bdm_id and cu.bd_id = hd.bd_id and cu.city_id = hd.city_id
left join 
active_merchant am 
on cu.hcm_id = am.hcm_id and cu.cm_id = am.cm_id and cu.rm_id = am.rm_id and cu.bdm_id = am.bdm_id and cu.bd_id = am.bd_id and cu.city_id = am.city_id
;


--03.将两个表的数据汇总到结果表
insert overwrite table opos_dw.app_opos_metrics_daily_new_d partition(country_code,dt)
select
o.id
,weekofyear('{pt}') as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year
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

,o.city_id
,nvl(c.name,'-') as city_name
,nvl(c.country,'-') as country

,o.merchant_cnt
,o.pos_merchant_cnt
,o.new_merchant_cnt
,o.new_pos_merchant_cnt
,o.pos_complete_order_cnt
,o.qr_complete_order_cnt
,o.complete_order_cnt
,o.gmv
,o.actual_amount

,o.pos_user_active_cnt
,o.qr_user_active_cnt
,o.before_1_day_user_active_cnt
,o.before_7_day_user_active_cnt
,o.before_15_day_user_active_cnt
,o.before_30_day_user_active_cnt
,o.order_merchant_cnt
,o.pos_order_merchant_cnt
,o.week_pos_user_active_cnt
,o.week_qr_user_active_cnt
,o.month_pos_user_active_cnt
,o.month_qr_user_active_cnt
,o.have_order_user_cnt

,o.return_amount
,o.new_user_cost
,o.old_user_cost
,o.return_amount_order_cnt

,o.user_active_cnt
,o.new_user_cnt
,o.more_5_merchant_cnt

,o.cashback_order_cnt
,o.cashback_fail_order_cnt
,o.cashback_order_gmv
,o.cashback_per_order_amt
,o.cashback_per_people_amt
,o.cashback_people_cnt
,o.cashback_first_people_cnt
,o.cashback_zero_order_cnt
,o.cashback_order_percent
,o.cashback_amt
,o.reduce_order_cnt
,o.reduce_zero_order_cnt
,o.reduce_amt
,o.reduce_order_gmv
,o.reduce_per_order_amt
,o.reduce_per_people_amt
,o.reduce_people_cnt
,o.reduce_first_people_cnt
,o.month_order_newshop_cnt

,'nal' as country_code
,'{pt}' as dt
from
  (
  select 
  0 as id
  ,substr(a.dt,0,7) as create_month
  ,substr(a.dt,0,4) as create_year
  
  ,nvl(a.hcm_id,b.hcm_id) as hcm_id
  ,nvl(a.cm_id,b.cm_id) as cm_id
  ,nvl(a.rm_id,b.rm_id) as rm_id
  ,nvl(a.bdm_id,b.bdm_id) as bdm_id
  ,nvl(a.bd_id,b.bd_id) as bd_id
  
  ,nvl(a.city_id,b.city_id) as city_id
  
  ,nvl(a.merchant_cnt,0) as merchant_cnt
  ,nvl(a.pos_merchant_cnt,0) as pos_merchant_cnt
  ,nvl(a.new_merchant_cnt,0) as new_merchant_cnt
  ,nvl(a.new_pos_merchant_cnt,0) as new_pos_merchant_cnt
  ,nvl(a.pos_complete_order_cnt,0) as pos_complete_order_cnt
  ,nvl(a.qr_complete_order_cnt,0) as qr_complete_order_cnt
  ,nvl(a.complete_order_cnt,0) as complete_order_cnt
  ,nvl(a.gmv,0) as gmv
  ,nvl(a.actual_amount,0) as actual_amount
  
  ,nvl(b.pos_user_active_cnt,0) as pos_user_active_cnt
  ,nvl(b.qr_user_active_cnt,0) as qr_user_active_cnt
  ,nvl(b.before_1_day_user_active_cnt,0) as before_1_day_user_active_cnt
  ,nvl(b.before_7_day_user_active_cnt,0) as before_7_day_user_active_cnt
  ,nvl(b.before_15_day_user_active_cnt,0) as before_15_day_user_active_cnt
  ,nvl(b.before_30_day_user_active_cnt,0) as before_30_day_user_active_cnt
  ,nvl(b.order_merchant_cnt,0) as order_merchant_cnt
  ,nvl(b.pos_order_merchant_cnt,0) as pos_order_merchant_cnt
  ,nvl(b.week_pos_user_active_cnt,0) as week_pos_user_active_cnt
  ,nvl(b.week_qr_user_active_cnt,0) as week_qr_user_active_cnt
  ,nvl(b.month_pos_user_active_cnt,0) as month_pos_user_active_cnt
  ,nvl(b.month_qr_user_active_cnt,0) as month_qr_user_active_cnt
  ,nvl(b.have_order_user_cnt,0) as have_order_user_cnt
  
  ,nvl(a.return_amount,0) as return_amount
  ,nvl(a.new_user_cost,0) as new_user_cost
  ,nvl(a.old_user_cost,0) as old_user_cost
  ,nvl(a.return_amount_order_cnt,0) as return_amount_order_cnt
  
  ,nvl(b.user_active_cnt,0) as user_active_cnt
  ,nvl(b.new_user_cnt,0) as new_user_cnt
  ,nvl(b.more_5_merchant_cnt,0) as more_5_merchant_cnt

  ,nvl(a.cashback_order_cnt,0) as cashback_order_cnt
  ,nvl(a.cashback_fail_order_cnt,0) as cashback_fail_order_cnt
  ,nvl(a.cashback_order_gmv,0) as cashback_order_gmv
  ,nvl(a.cashback_per_order_amt,0) as cashback_per_order_amt
  ,nvl(a.cashback_per_people_amt,0) as cashback_per_people_amt
  ,nvl(a.cashback_people_cnt,0) as cashback_people_cnt
  ,nvl(a.cashback_first_people_cnt,0) as cashback_first_people_cnt
  ,nvl(a.cashback_zero_order_cnt,0) as cashback_zero_order_cnt
  ,nvl(a.cashback_order_percent,0) as cashback_order_percent
  ,nvl(a.cashback_amt,0) as cashback_amt
  
  ,nvl(a.reduce_order_cnt,0) as reduce_order_cnt
  ,nvl(a.reduce_zero_order_cnt,0) as reduce_zero_order_cnt
  ,nvl(a.reduce_amt,0) as reduce_amt
  ,nvl(a.reduce_order_gmv,0) as reduce_order_gmv
  ,nvl(a.reduce_per_order_amt,0) as reduce_per_order_amt
  ,nvl(a.reduce_per_people_amt,0) as reduce_per_people_amt
  ,nvl(a.reduce_people_cnt,0) as reduce_people_cnt
  ,nvl(a.reduce_first_people_cnt,0) as reduce_first_people_cnt

  ,nvl(b.month_order_newshop_cnt,0) as month_order_newshop_cnt

  from 
  (select * from opos_dw.app_opos_report_mid where country_code = 'nal' and  dt = '{pt}') a
  full join 
  (select * from opos_dw.app_opos_active_user_daily_mid where country_code = 'nal' and  dt = '{pt}') b 
  on  
  a.hcm_id=b.hcm_id
  AND a.cm_id=b.cm_id
  AND a.rm_id=b.rm_id
  AND a.bdm_id=b.bdm_id
  AND a.bd_id=b.bd_id
  and a.city_id=b.city_id
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
  (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as c
on o.city_id=c.id
;








'''.format(
        pt=ds,
        table=table_name,
        before_1_day=airflow.macros.ds_add(ds, -1),
        before_7_day=airflow.macros.ds_add(ds, -7),
        before_15_day=airflow.macros.ds_add(ds, -15),
        before_30_day=airflow.macros.ds_add(ds, -30),
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opos_metrics_daily_new_d_sql_task(ds)

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


app_opos_metrics_daily_new_d_task = PythonOperator(
    task_id='app_opos_metrics_daily_new_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_pre_opos_payment_order_di_task >> app_opos_metrics_daily_new_d_task
ods_sqoop_base_bd_admin_users_df_task >> app_opos_metrics_daily_new_d_task

