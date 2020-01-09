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
                  schedule_interval="30 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

app_opos_metrics_report_mid_task = OssSensor(
    task_id='app_opos_metrics_report_mid_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/app_opos_metrics_report_mid",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

app_opos_metrics_daily_mid_task = OssSensor(
    task_id='app_opos_metrics_daily_mid_task',
    bucket_key='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/app_opos_metrics_daily_mid",
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

def app_opos_metrics_daily_new_d_sql_task(ds):
    HQL = '''


set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

with
admin_users as (
  select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}'
)

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
,o.month_newshop_cnt

,o.order_create_cnt
,o.order_pay_cnt
,o.order_fail_cnt
,o.order_pending_cnt

,o.coupon_order_cnt
,o.coupon_order_people
,o.coupon_first_order_people
,o.coupon_pay_amount
,o.coupon_order_gmv
,o.coupon_discount_amount
,o.coupon_useless_order_cnt
,o.coupon_useless_order_people
,o.coupon_useless_pay_amount
,o.coupon_useless_order_gmv

,o.coupon_amount
,o.coupon_used_amount
,o.coupon_expired_amount
,o.coupon_invalid_amount

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
  ,nvl(a.month_newshop_cnt,0) as month_newshop_cnt

  ,nvl(b.order_create_cnt,0) as order_create_cnt
  ,nvl(b.order_pay_cnt,0) as order_pay_cnt
  ,nvl(b.order_fail_cnt,0) as order_fail_cnt
  ,nvl(b.order_pending_cnt,0) as order_pending_cnt

  ,nvl(a.coupon_order_cnt,0) as coupon_order_cnt
  ,nvl(a.coupon_order_people,0) as coupon_order_people
  ,nvl(a.coupon_first_order_people,0) as coupon_first_order_people
  ,nvl(a.coupon_pay_amount,0) as coupon_pay_amount
  ,nvl(a.coupon_order_gmv,0) as coupon_order_gmv
  ,nvl(a.coupon_discount_amount,0) as coupon_discount_amount
  ,nvl(a.coupon_useless_order_cnt,0) as coupon_useless_order_cnt
  ,nvl(a.coupon_useless_order_people,0) as coupon_useless_order_people
  ,nvl(a.coupon_useless_pay_amount,0) as coupon_useless_pay_amount
  ,nvl(a.coupon_useless_order_gmv,0) as coupon_useless_order_gmv

  ,nvl(a.coupon_amount,0) as coupon_amount
  ,nvl(a.coupon_used_amount,0) as coupon_used_amount
  ,nvl(a.coupon_expired_amount,0) as coupon_expired_amount
  ,nvl(a.coupon_invalid_amount,0) as coupon_invalid_amount

  from 
  (select * from opos_dw.app_opos_metrics_report_mid where country_code = 'nal' and  dt = '{pt}') a
  full join 
  (select * from opos_dw.app_opos_metrics_daily_mid where country_code = 'nal' and  dt = '{pt}') b 
  on  
  a.hcm_id=b.hcm_id
  AND a.cm_id=b.cm_id
  AND a.rm_id=b.rm_id
  AND a.bdm_id=b.bdm_id
  AND a.bd_id=b.bd_id
  and a.city_id=b.city_id
  ) as o
left join
  admin_users as bd
  on o.bd_id=bd.id
left join
  admin_users as bdm
on o.bdm_id=bdm.id
left join
  admin_users as rm
on o.rm_id=rm.id
left join
  admin_users as cm
on o.cm_id=cm.id
left join
  admin_users as hcm
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

app_opos_metrics_report_mid_task >> app_opos_metrics_daily_new_d_task
app_opos_metrics_daily_mid_task >> app_opos_metrics_daily_new_d_task

