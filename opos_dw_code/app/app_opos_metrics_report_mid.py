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

dag = airflow.DAG('app_opos_metrics_report_mid',
                  schedule_interval="10 02 * * *",
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
table_name = "app_opos_metrics_report_mid"
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

def app_opos_metrics_report_mid_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--01.统计商铺信息,新增商铺数等等
with
shop_cnt as (
  select 
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

  --月新增商户数
  ,count(distinct(if(created_at>=concat(substr('{pt}',0,7),'-01') and created_at<='{pt}',id,null))) as month_newshop_cnt
  --月激活商户数
  ,count(if(activate_month=substr('{pt}',0,7),id,null)) as activate_shop_cnt

  --封禁商户数
  ,count(if(status='3',id,null)) as prohibit_shop_cnt
  ,0 as pos_prohibit_shop_cnt

  --有效商户数
  ,count(if(status!='3',id,null)) as effective_shop_cnt
  ,0 as pos_effective_shop_cnt

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
  
),

--02.优惠券数据
coupon_info as (
  select 
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_id
  
  ,sum(if(coupon_type!='BONUS',amount,0)) as coupon_amount
  ,sum(if(coupon_type!='BONUS' and status='USED',amount,0)) as coupon_used_amount
  ,sum(if(coupon_type!='BONUS' and status='EXPIRED',amount,0)) as coupon_expired_amount
  ,sum(if(coupon_type!='BONUS' and status='INVALID',amount,0)) as coupon_invalid_amount
  from 
  opos_dw.dwd_opos_coupon_di as p
  where 
  country_code='nal'
  and dt = '{pt}'
  group by
  hcm_id
  ,cm_id
  ,rm_id
  ,bdm_id
  ,bd_id
  
  ,city_id
),

--03.shop和优惠券混合
shop_coupon as (
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
,nvl(a.month_newshop_cnt,0) as month_newshop_cnt

,nvl(a.activate_shop_cnt,0) as activate_shop_cnt

,nvl(a.prohibit_shop_cnt,0) as prohibit_shop_cnt
,nvl(a.pos_prohibit_shop_cnt,0) as pos_prohibit_shop_cnt

,nvl(b.coupon_amount,0) as coupon_amount
,nvl(b.coupon_used_amount,0) as coupon_used_amount
,nvl(b.coupon_expired_amount,0) as coupon_expired_amount
,nvl(b.coupon_invalid_amount,0) as coupon_invalid_amount
from
shop_cnt as a
full join
coupon_info as b
on
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id
),

--04.根据销售信息统计bd销售指标
payment_info as (
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
  ,nvl(sum(if(first_order = '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0) + nvl(discount_amount,0),0)),0) as new_user_cost
  ,nvl(sum(if(first_order <> '1',nvl(org_payment_amount,0) - nvl(pay_amount,0) + nvl(user_subsidy,0) + nvl(discount_amount,0),0)),0) as old_user_cost
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
  
  --优惠券分析
  ,count(if(nvl(discount_type,'')!='',1,null)) as coupon_order_cnt
  ,count(distinct(if(nvl(discount_type,'')!='',sender_id,null))) as coupon_order_people
  ,count(distinct(if(nvl(discount_type,'')!='' and first_order='1',sender_id,null))) as coupon_first_order_people
  ,sum(if(nvl(discount_type,'')!='',pay_amount,0)) as coupon_pay_amount
  ,sum(if(nvl(discount_type,'')!='',org_payment_amount,0)) as coupon_order_gmv
  ,sum(if(nvl(discount_type,'')!='',discount_amount,0)) as coupon_discount_amount

  ,count(if(nvl(discount_type,'')='',1,null)) as coupon_useless_order_cnt
  ,count(distinct(if(nvl(discount_type,'')='',sender_id,null))) as coupon_useless_order_people
  ,sum(if(nvl(discount_type,'')='',pay_amount,0)) as coupon_useless_pay_amount
  ,sum(if(nvl(discount_type,'')='',org_payment_amount,0)) as coupon_useless_order_gmv

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
)

--05.最后向report临时表中插入当日销售和订单的数据
insert overwrite table opos_dw.app_opos_metrics_report_mid partition (country_code,dt)
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

,nvl(a.month_newshop_cnt,0) as month_newshop_cnt

,nvl(b.coupon_order_cnt,0) as coupon_order_cnt
,nvl(b.coupon_order_people,0) as coupon_order_people
,nvl(b.coupon_first_order_people,0) as coupon_first_order_people
,nvl(b.coupon_pay_amount,0) as coupon_pay_amount
,nvl(b.coupon_order_gmv,0) as coupon_order_gmv
,nvl(b.coupon_discount_amount,0) as coupon_discount_amount

,nvl(b.coupon_useless_order_cnt,0) as coupon_useless_order_cnt
,nvl(b.coupon_useless_order_people,0) as coupon_useless_order_people
,nvl(b.coupon_useless_pay_amount,0) as coupon_useless_pay_amount
,nvl(b.coupon_useless_order_gmv,0) as coupon_useless_order_gmv

,nvl(a.coupon_amount,0) as coupon_amount
,nvl(a.coupon_used_amount,0) as coupon_used_amount
,nvl(a.coupon_expired_amount,0) as coupon_expired_amount
,nvl(a.coupon_invalid_amount,0) as coupon_invalid_amount

,nvl(a.activate_shop_cnt,0) as activate_shop_cnt

,nvl(a.prohibit_shop_cnt,0) as prohibit_shop_cnt
,nvl(a.pos_prohibit_shop_cnt,0) as pos_prohibit_shop_cnt

,'nal' as country_code
,'{pt}' as dt
from
  shop_coupon as a
full join
  payment_info as b
on
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id
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
    _sql = app_opos_metrics_report_mid_sql_task(ds)

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


app_opos_metrics_report_mid_task = PythonOperator(
    task_id='app_opos_metrics_report_mid_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_pre_opos_payment_order_di_task >> app_opos_metrics_report_mid_task
ods_sqoop_base_bd_admin_users_df_task >> app_opos_metrics_report_mid_task

