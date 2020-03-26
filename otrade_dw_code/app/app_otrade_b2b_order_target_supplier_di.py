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
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'yuanfeng',
    'start_date': datetime(2020, 3, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_otrade_b2b_order_target_supplier_di',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "app_otrade_b2b_order_target_supplier_di"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##

dwd_otrade_b2b_otrade_order_di_task = OssSensor(
    task_id='dwd_otrade_b2b_otrade_order_di_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_otrade_order_di",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag": dag, "db": "otrade_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_otrade_b2b_order_target_supplier_di_sql_task(ds):
    HQL = '''

set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--0.查看店铺信息
with
order_info as (
  select
    shop_id
    ,payee
    ,shop_name

    ,hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_id
    ,bd_name
  
    ,country
    ,country_name
    ,city
    ,city_name
  
    --下单分析
    ,sum(if(order_type='pay',payable_amount,0)) as order_amt
    ,count(if(order_type='pay',1,null)) as order_cnt
    ,count(distinct(if(order_type='pay',payer,null))) as order_people
    ,sum(if(order_type='pay' and retailer_first_order=1,payable_amount,0)) as first_order_amt
    ,count(if(order_type='pay' and retailer_first_order=1,1,null)) as first_order_cnt
    ,count(distinct(if(order_type='pay' and retailer_first_order=1,payer,null))) as first_order_people
  
    --销售分析
    ,sum(if(order_type='pay',amount,0)) as pay_amt
    ,count(if(order_type='pay' and pay_status = 3,1,null)) as pay_suc_cnt
    ,count(if(order_type='pay' and pay_time is not null,1,null)) as pay_cnt
  
    --退款分析
    ,count(if(order_type='pay' and order_status=0,1,null)) as refund_order_cnt
    ,count(if(order_type='refund' and order_status=2,1,null)) as refund_suc_cnt
    ,sum(if(order_type='refund' and order_status=2,payable_amount,0)) as refund_order_amt
  
    --收货分析
    ,count(if(order_type='pay' and consign_time='{pt}',1,null)) as delivery_order_cnt
    ,count(if(order_type='pay' and confirm_time='{pt}',1,null)) as receive_order_cnt
    ,sum(if(order_type='pay' and confirm_time='{pt}',payable_amount,0)) as receive_order_amt
  
    --用户分析
    ,count(distinct(if(order_type='pay' and pay_time is not null,payer,null))) as pay_people
    ,count(distinct(if(order_type='pay' and pay_status = 3,payer,null))) as pay_suc_people
  
    ,country_code
    ,dt
  from
    otrade_dw.dwd_otrade_b2b_otrade_order_di
  where
    dt = '{pt}'
  group by
    shop_id
    ,payee
    ,shop_name

    ,hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_id
    ,bd_name
    ,country
    ,country_name
    ,city
    ,city_name
    
    ,country_code
    ,dt
)

--2.插入数据
insert overwrite table otrade_dw.app_otrade_b2b_order_target_supplier_di partition(country_code,dt)
select
  shop_id
  ,payee
  ,shop_name
  ,hcm_id
  ,hcm_name
  ,cm_id
  ,cm_name
  ,bdm_id
  ,bdm_name
  ,bd_id
  ,bd_name
  ,country
  ,country_name
  ,city
  ,city_name

  ,order_amt
  ,order_cnt
  ,order_people
  ,first_order_amt
  ,first_order_cnt
  ,first_order_people
  ,pay_amt
  ,pay_suc_cnt
  ,pay_cnt
  ,refund_order_cnt
  ,refund_suc_cnt
  ,refund_order_amt
  ,delivery_order_cnt
  ,receive_order_cnt
  ,receive_order_amt
  ,pay_people
  ,pay_suc_people

  ,'NG' as country_code
  ,'{pt}' as dt
from
  order_info as v1
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
    _sql = app_otrade_b2b_order_target_supplier_di_sql_task(ds)

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
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "false")


app_otrade_b2b_order_target_supplier_di_task = PythonOperator(
    task_id='app_otrade_b2b_order_target_supplier_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_otrade_b2b_otrade_order_di_task >> app_otrade_b2b_order_target_supplier_di_task





