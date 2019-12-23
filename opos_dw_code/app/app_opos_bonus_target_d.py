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

dag = airflow.DAG('app_opos_bonus_target_d',
                  schedule_interval="10 03 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区，dim_opos_bd_relation_df表，ufile://opay-datalake/opos/opos_dw/dim_opos_bd_relation_df
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
table_name = "app_opos_bonus_target_d"
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

def app_opos_bonus_target_d_sql_task(ds):
    HQL = '''
  
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--02.先取红包record表
with
app_opos_bonus_target_d as (
select
city_id as city_code,

hcm_id,
cm_id,
rm_id,
bdm_id,
bd_id,

--已入账金额
sum(if(status=1,nvl(bonus_amount,0),0)) as entered_account_amt,
--待入账金额
sum(if(status=0,nvl(bonus_amount,0),0)) as tobe_entered_account_amt,
--被扫者奖励金额
sum(nvl(bonus_amount,0)) as swepted_award_amt,
--已结算金额
sum(if(status=1 and settle_status=1,nvl(bonus_amount,0),0)) as settled_amount,
--待结算金额
sum(if(status=1 and settle_status=0,nvl(bonus_amount,0),0)) as tobe_settled_amount,
--获取金额的被扫者
count(distinct(provider_account)) as swepted_award_cnt,

--扫描红包二维码次数
count(1) as sweep_times,
--扫描红包二维码人数
count(distinct(opay_account)) as sweep_people,
--主扫红包金额
sum(nvl(amount,0)) as sweep_amt,

--红包使用率
sum(nvl(use_amount,0))/sum(nvl(bonus_amount,0)) as bonus_use_percent,
--红包使用金额
sum(nvl(use_amount,0)) as bonus_order_amt,

'nal' as country_code,
'{pt}' as dt
from
opos_dw.dwd_opos_bonus_record_di
where 
country_code='nal' 
and dt='{pt}'
group by
city_id,

hcm_id,
cm_id,
rm_id,
bdm_id,
bd_id


),

app_opos_bonus_payment_target_d as (
select
city_id as city_code,

hcm_id,
cm_id,
rm_id,
bdm_id,
bd_id,

--红包订单数量
count(if(length(discount_ids)>0,1,null)) as bonus_order_cnt,
--红包订单GMV
sum(if(length(discount_ids)>0,nvl(org_payment_amount,0),0)) as bonus_order_gmv,
--首单用户占比（红包）,判断是否是首单需要确认,暂定0和1
count(distinct(if(length(discount_ids)>0 and first_order='1',sender_id,null)))/count(distinct(if(length(discount_ids)>0,sender_id,null))) as first_order_percent,
--红包支付用户数
count(distinct(if(length(discount_ids)>0,sender_id,null))) as bonus_order_people,
--红包支付次数
count(if(length(discount_ids)>0,1,null)) as bonus_order_times,

--人均消费金额增幅（红包用户）,用红包支付的人均应付金额-没用红包支付的人均应付金额
sum(if(length(discount_ids)>0,nvl(org_payment_amount,0),0))/count(distinct(if(length(discount_ids)>0,sender_id,null))) 
  - sum(if(length(discount_ids)=0,nvl(org_payment_amount,0),0))/count(distinct(if(length(discount_ids)=0,sender_id,null))) as people_avg_amt_increase,
--订单均消费金额增幅（红包用户）,用红包支付的单均应付金额-没用红包支付的单均应付金额,这个分母不需要加distinct,
sum(if(length(discount_ids)>0,nvl(org_payment_amount,0),0))/count(if(length(discount_ids)>0,sender_id,null)) 
  - sum(if(length(discount_ids)=0,nvl(org_payment_amount,0),0))/count(if(length(discount_ids)=0,sender_id,null)) as order_avg_amt_increase,
--单均实付增幅（红包用户）,用实付
sum(if(length(discount_ids)>0,nvl(pay_amount,0),0))/count(if(length(discount_ids)>0,sender_id,null)) 
  - sum(if(length(discount_ids)=0,nvl(pay_amount,0),0))/count(if(length(discount_ids)=0,sender_id,null)) as order_avg_realamt_increase,
--交易频次增幅（红包用户）,【使用红包支付订单的】交易频次-【不使用红包支付订单的】交易频次
count(if(length(discount_ids)>0,1,null)) 
  - count(if(length(discount_ids)=0,1,null)) as order_times_increase,
--首单用户数（红包用户）
count(distinct(if(length(discount_ids)>0 and first_order='1',sender_id,null))) as first_order_people,

--新增字段
--没用红包支付的应付金额
sum(if(length(discount_ids)=0,nvl(org_payment_amount,0),0)) as useless_org_payment_amt,
--没用红包支付的用户数
count(distinct(if(length(discount_ids)=0,sender_id,null))) as useless_people,
--没用红包支付的订单数
count(if(length(discount_ids)=0,sender_id,null)) as useless_order_cnt,

--用红包支付的实付总金额
sum(if(length(discount_ids)>0,nvl(pay_amount,0),0)) as bonus_real_amt,
--没用红包支付的实付总金额
sum(if(length(discount_ids)=0,nvl(pay_amount,0),0)) as useless_bonus_real_amt,

'nal' as country_code,
'{pt}' as dt

from
opos_dw.dwd_pre_opos_payment_order_di
where 
country_code='nal' 
and dt='{pt}'
and trade_status='SUCCESS'
group by
city_id,

hcm_id,
cm_id,
rm_id,
bdm_id,
bd_id

)

insert overwrite table opos_dw.app_opos_bonus_target_d partition(country_code,dt)
select

substr('{pt}',0,10) as create_date
,weekofyear('{pt}') as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year

,o.city_code
,nvl(c.name,'-') as city_name
,nvl(c.country,'-') as country

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

,o.entered_account_amt
,o.tobe_entered_account_amt
,o.swepted_award_amt
,o.settled_amount
,o.tobe_settled_amount
,o.swepted_award_cnt
,o.sweep_times
,o.sweep_people
,o.sweep_amt
,o.bonus_use_percent
,o.bonus_order_amt

,o.bonus_order_cnt
,o.bonus_order_gmv
,o.first_order_percent
,o.bonus_order_people
,o.bonus_order_times
,o.people_avg_amt_increase
,o.order_avg_amt_increase
,o.order_avg_realamt_increase
,o.order_times_increase
,o.first_order_people
,o.useless_org_payment_amt
,o.useless_people
,o.useless_order_cnt
,o.bonus_real_amt
,o.useless_bonus_real_amt

,'nal' as country_code
,'{pt}' as dt
from
  (
  select
  nvl(a.city_code,b.city_code) as city_code
  
  ,nvl(a.hcm_id,b.hcm_id) as hcm_id
  ,nvl(a.cm_id,b.cm_id) as cm_id
  ,nvl(a.rm_id,b.rm_id) as rm_id
  ,nvl(a.bdm_id,b.bdm_id) as bdm_id
  ,nvl(a.bd_id,b.bd_id) as bd_id
  
  ,nvl(b.entered_account_amt,0) as entered_account_amt
  ,nvl(b.tobe_entered_account_amt,0) as tobe_entered_account_amt
  ,nvl(b.swepted_award_amt,0) as swepted_award_amt
  ,nvl(b.settled_amount,0) as settled_amount
  ,nvl(b.tobe_settled_amount,0) as tobe_settled_amount
  ,nvl(b.swepted_award_cnt,0) as swepted_award_cnt
  ,nvl(b.sweep_times,0) as sweep_times
  ,nvl(b.sweep_people,0) as sweep_people
  ,nvl(b.sweep_amt,0) as sweep_amt
  ,nvl(b.bonus_use_percent,0) as bonus_use_percent
  ,nvl(b.bonus_order_amt,0) as bonus_order_amt
  
  ,nvl(a.bonus_order_cnt,0) as bonus_order_cnt
  ,nvl(a.bonus_order_gmv,0) as bonus_order_gmv
  ,nvl(a.first_order_percent,0) as first_order_percent
  ,nvl(a.bonus_order_people,0) as bonus_order_people
  ,nvl(a.bonus_order_times,0) as bonus_order_times
  ,nvl(a.people_avg_amt_increase,0) as people_avg_amt_increase
  ,nvl(a.order_avg_amt_increase,0) as order_avg_amt_increase
  ,nvl(a.order_avg_realamt_increase,0) as order_avg_realamt_increase
  ,nvl(a.order_times_increase,0) as order_times_increase
  ,nvl(a.first_order_people,0) as first_order_people
  ,nvl(a.useless_org_payment_amt,0) as useless_org_payment_amt
  ,nvl(a.useless_people,0) as useless_people
  ,nvl(a.useless_order_cnt,0) as useless_order_cnt
  ,nvl(a.bonus_real_amt,0) as bonus_real_amt
  ,nvl(a.useless_bonus_real_amt,0) as useless_bonus_real_amt
  from
    app_opos_bonus_payment_target_d as a 
  full join
    app_opos_bonus_target_d as b
  on  
    a.hcm_id=b.hcm_id
    AND a.cm_id=b.cm_id
    AND a.rm_id=b.rm_id
    AND a.bdm_id=b.bdm_id
    AND a.bd_id=b.bd_id
    and a.city_code=b.city_code
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
on o.city_code=c.id
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
    _sql = app_opos_bonus_target_d_sql_task(ds)

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


app_opos_bonus_target_d_task = PythonOperator(
    task_id='app_opos_bonus_target_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_opos_bonus_record_di_task >> app_opos_bonus_target_d_task
dwd_pre_opos_payment_order_di_task >> app_opos_bonus_target_d_task

# 查看任务命令
# airflow list_tasks app_opos_bonus_target_d -sd /home/feng.yuan/app_opos_bonus_target_d.py
# 测试任务命令
# airflow test app_opos_bonus_target_d app_opos_bonus_target_d_task 2019-11-24 -sd /home/feng.yuan/app_opos_bonus_target_d.py

