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

# 依赖前一天分区，dim_opos_bd_relation_df表，ufile://opay-datalake/opos/opos_dw/dim_opos_bd_relation_df
dwd_opos_bonus_record_di_task = UFileSensor(
    task_id='dwd_opos_bonus_record_di_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_dw/dwd_opos_bonus_record_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

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
table_name = "app_opos_bonus_shop_target_d"
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

def app_opos_bonus_shop_target_d_sql_task(ds):
    HQL = '''
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;

--02.用shop表计算出每个bd下有
--得出最新维度下每个dbid的详细数据信息
insert overwrite table opos_dw.app_opos_bonus_shop_target_d partition (country_code,dt)
select
a.shop_id
,a.opay_id
,a.shop_name
,a.opay_account
,a.create_date
,a.create_week
,a.create_month
,a.create_year
,a.city_id
,a.city_name
,a.country
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

,nvl(a.order_cnt,0) as order_cnt
,nvl(a.bonus_order_cnt,0) as bonus_order_cnt
,nvl(a.order_people,0) as order_people
,nvl(a.not_first_order_people,0) as not_first_order_people
,nvl(a.first_order_people,0) as first_order_people
,nvl(a.first_bonus_order_people,0) as first_bonus_order_people
,nvl(a.order_gmv,0) as order_gmv
,nvl(a.bonus_order_gmv,0) as bonus_order_gmv

,b.bonus_order_amt
,b.sweep_amt
,b.bonus_use_percent

,nvl(a.bonus_order_people,0) as bonus_order_people
,nvl(a.bonus_order_times,0) as bonus_order_times
,nvl(a.order_avg_amt,0) as order_avg_amt
,nvl(a.people_avg_amt,0) as people_avg_amt


,'nal' as country_code
,'{pt}' as dt
from
(
  select
  shop_id
  ,receipt_id as opay_id
  ,shop_name
  ,opay_account
  
  ,create_date
  ,create_week
  ,create_month
  ,create_year
  
  ,city_id_shop as city_id
  ,city_name_shop as city_name
  ,country_shop as country
  
  ,hcm_id
  ,hcm_name
  ,cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name
  
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
  (select * from opos_dw.dwd_pre_opos_payment_order_di where country_code='nal' and dt='{pt}' and trade_status='SUCCESS') as m
  group BY
  shop_id
  ,receipt_id
  ,shop_name
  ,opay_account
  
  ,create_date
  ,create_week
  ,create_month
  ,create_year
  
  ,city_id_shop
  ,city_name_shop
  ,country_shop
  
  ,hcm_id
  ,hcm_name
  ,cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name

) as a

left join

(
  select
  provider_shop_id as shop_id
  ,provider_opay_id as opay_id
  ,provider_shop_name as shop_name
  ,provider_account as opay_account
  
  ,create_date
  ,create_week
  ,create_month
  ,create_year
  
  ,provider_city_id as city_id
  ,provider_city_name as city_name
  ,provider_country as country
  
  ,hcm_id
  ,hcm_name
  ,cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name

  ,sum(use_amount) as bonus_order_amt
  ,sum(bonus_amount) as sweep_amt
  ,nvl(sum(use_amount)/sum(bonus_amount),0) as bonus_use_percent
  from
  (select * from opos_dw.dwd_opos_bonus_record_di where country_code='nal' and dt='{pt}') as n
  group BY
  provider_shop_id
  ,provider_opay_id
  ,provider_shop_name
  ,provider_account
  
  ,create_date
  ,create_week
  ,create_month
  ,create_year
  
  ,provider_city_id
  ,provider_city_name
  ,provider_country
  
  ,hcm_id
  ,hcm_name
  ,cm_id
  ,cm_name
  ,rm_id
  ,rm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name

) as b

on
a.shop_id=b.shop_id
and a.create_date=b.create_date
and a.city_id=b.city_id
and a.bd_id=b.bd_id
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

