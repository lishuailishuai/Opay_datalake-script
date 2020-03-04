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
    'start_date': datetime(2019, 11, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_pre_opos_payment_order_di',
                  schedule_interval="30 01 * * *",
                  default_args=args,
                  )

##----------------------------------------- 依赖 ---------------------------------------##

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

ods_sqoop_base_pre_opos_payment_order_di_task = OssSensor(
    task_id='ods_sqoop_base_pre_opos_payment_order_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop_di/pre_ptsp_db/pre_opos_payment_order",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

ods_sqoop_base_pre_opos_payment_order_bd_di_task = OssSensor(
    task_id='ods_sqoop_base_pre_opos_payment_order_bd_di_task',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos_dw_sqoop_di/pre_ptsp_db/pre_opos_payment_order_bd",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "dwd_pre_opos_payment_order_di"
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

def dwd_pre_opos_payment_order_di_sql_task(ds):
    HQL = '''


--插入数据
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;


insert overwrite table opos_dw.dwd_pre_opos_payment_order_di partition(country_code,dt)
select
p.order_id
,p.device_no
,p.cfrom

,substr('{pt}',0,10) as create_date
,weekofyear('{pt}') as create_week
,substr('{pt}',0,7) as create_month
,substr('{pt}',0,4) as create_year

,p.receipt_id
,p.sender_id

,nvl(bd.hcm_id,0) as hcm_id
,nvl(bd.hcm_name,'-') as hcm_name
,nvl(bd.cm_id,0) as cm_id
,nvl(bd.cm_name,'-') as cm_name
,nvl(bd.rm_id,0) as rm_id
,nvl(bd.rm_name,'-') as rm_name
,nvl(bd.bdm_id,0) as bdm_id
,nvl(bd.bdm_name,'-') as bdm_name
,nvl(bd.bd_id,0) as bd_id
,nvl(bd.bd_name,'-') as bd_name

,nvl(bd.city_id,'-') as city_id
,nvl(bd.name,'-') as city_name
,nvl(bd.country,'-') as country

,nvl(bd.shop_id,'-') as shop_id
,nvl(bd.shop_name,'-') as shop_name
,nvl(bd.opay_account,'-') as opay_account
,nvl(bd.contact_name,'-') as contact_name
,nvl(bd.contact_phone,'-') as contact_phone
,nvl(bd.cate_id,'-') as cate_id
,nvl(bd.created_at,'-') as created_at

,nvl(bd.city_id_shop,'-') as city_id_shop
,nvl(bd.city_name_shop,'-') as city_name_shop
,nvl(bd.country_shop,'-') as country_shop

,p.bill_create_ip
,p.org_pp_trade_no
,p.pp_trade_no
,p.payment_id
,p.org_payment_amount
,p.pay_type
,p.pay_amount
,p.merchant_activity_id
,p.merchant_activity_type
,p.merchant_activity_title
,p.threshold_amount
,p.threshold_orders
,p.activity_type
,p.activity_title
,p.activity_id
,p.discount_ids
,p.discount_amount
,p.return_amount
,p.user_subsidy
,p.order_type
,p.pay_cur
,p.trade_type
,p.trade_status
,p.merchant_subsidy_status
,p.user_subsidy_status
,p.first_order
,p.resp_code
,p.resp_message
,p.query_resp_code
,p.query_resp_message
,p.auth_code
,p.trade_version
,p.reversal_type
,p.refund_code
,p.repaired
,p.create_time
,p.modify_time
,p.resp_time
,p.goods_desc
,p.remark
,p.sn
,p.pos_user_data
,p.user_risk_status
,p.user_risk_code
,p.user_risk_remark
,p.merchant_risk_status
,p.merchant_risk_code
,p.merchant_risk_remark

,p.delay_settle
,p.settle_time
,p.settled
,p.user_phone
,p.shop_phone
,p.merchant_gain
,p.promote_type
,p.merchant_cost
,p.platform_cost
,p.bill_settle_time
,p.merchant_qr_id
,p.discount_type

,'nal' as country_code
,'{pt}' as dt
from
(select * from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_di where dt = '{pt}') as p 
left join
    --先用orderod关联每一笔交易的bd_id,只取能关联上bd信息的交易，故用inner join
(
  select 
  s.order_id
  ,s.city_id
  ,s.shop_id
  ,ci.name
  ,ci.country

  ,s.hcm_id
  ,hcm.name as hcm_name
  ,s.cm_id
  ,cm.name as cm_name
  ,s.rm_id
  ,rm.name as rm_name
  ,s.bdm_id
  ,bdm.name as bdm_name
  ,s.bd_id
  ,bd2.name as bd_name

  ,shop.shop_name
  ,shop.opay_account
  ,shop.contact_name
  ,shop.contact_phone
  ,shop.cate_id
  ,shop.created_at
  ,shop.city_code as city_id_shop
  ,shop.city_name as city_name_shop
  ,shop.country as country_shop 
  from
    (select * from opos_dw_ods.ods_sqoop_base_pre_opos_payment_order_bd_di where dt='{pt}') as s 
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bd2
  on s.bd_id=bd2.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as bdm
  on s.bdm_id=bdm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as rm
  on s.rm_id=rm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as cm
  on s.cm_id=cm.id
  left join
    (select id,name from opos_dw_ods.ods_sqoop_base_bd_admin_users_df where dt = '{pt}') as hcm
  on s.hcm_id=hcm.id
  left join
  --关联城市码表，求出国家和城市描述
    (select id,name,country from opos_dw_ods.ods_sqoop_base_bd_city_df where dt = '{pt}') as ci
  on s.city_id=ci.id
  left join
    (select * from opos_dw.dim_opos_bd_relation_df where country_code='nal' and dt='{pt}') as shop
  on s.shop_id=shop.id
) as bd
on
  p.order_id=bd.order_id;





'''.format(
        pt=ds,
        table=table_name,
        now_day=airflow.macros.ds_add(ds, +1),
        before_1_day=airflow.macros.ds_add(ds, -1),
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_pre_opos_payment_order_di_sql_task(ds)

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


dwd_pre_opos_payment_order_di_task = PythonOperator(
    task_id='dwd_pre_opos_payment_order_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dim_opos_bd_relation_df_task >> dwd_pre_opos_payment_order_di_task
ods_sqoop_base_pre_opos_payment_order_di_task >> dwd_pre_opos_payment_order_di_task
ods_sqoop_base_pre_opos_payment_order_bd_di_task >> dwd_pre_opos_payment_order_di_task

# 查看任务命令
# airflow list_tasks dwd_pre_opos_payment_order_di -sd /home/feng.yuan/dwd_pre_opos_payment_order_di.py
# 测试任务命令
# airflow test dwd_pre_opos_payment_order_di dwd_pre_opos_payment_order_di_task 2019-11-24 -sd /home/feng.yuan/dwd_pre_opos_payment_order_di.py


