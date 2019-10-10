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
from plugins.ModelPublicFrame import ModelPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 5, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwd_oride_order_pay_detail_di', 
    schedule_interval="30 01 * * *", 
    default_args=args,
    catchup=False) 


sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)


##----------------------------------------- 变量 ---------------------------------------##

table_name="dwd_oride_order_pay_detail_di"

table_list = [
        {"db": "oride_dw", "table":table_name, "partitions": "country_code=nal", "timeout": "1600"}
    ]

##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,**op_kwargs):

    ModelPublicFrame().task_timeout_monitor(table_list,ds)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 依赖 ---------------------------------------## 


dependence_table_lists = [

{"db": "oride_dw", "table": "dwd_oride_order_base_include_test_di", "partitions": "country_code=nal"},

{"db": "oride_dw_ods", "table": "ods_sqoop_base_data_order_payment_df", "partitions": "country_code=nal"}

]

##----------------------------------------- 脚本 ---------------------------------------## 

dwd_oride_order_pay_detail_di_task = HiveOperator(

    task_id='dwd_oride_order_pay_detail_di_task',
    hql='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select 
            pay.order_id,--订单 ID
            driver_id,--司机ID
            pay_mode,--支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
            price,-- 价格
            coupon_id,--使用的优惠券 ID
            coupon_name ,--优惠券名称
            coupon_amount,-- 使用的优惠券金额
            pay_amount,--实付金额
            bonus,-- 使用的奖励金
            balance,--使用的余额
            opay_amount,--opay 支付的金额
            reference,-- opay 流水号
            currency,--货币类型
            country,--国家
            status,--支付模式（0: 支付中, 1: 成功, 2: 失败）
            modify_time,--最后修改时间
            create_time,--创建时间
            product_id,--订单业务类型(0: all 1:driect 2: street)
            updated_time,--最后更新时间
            pay_type, --支付类型(0:手动支付 1:自动支付)
            city_id,--所属城市
            passenger_id, --乘客 ID
            country_code,
            '{pt}' as dt
from 
     (SELECT 
            
            id as order_id,--订单 ID
            driver_id,--司机ID
            mode as pay_mode,--支付模式（0: 未知, 1: 线下支付, 2: opay, 3: 余额）
            price,-- 价格
            coupon_id,--使用的优惠券 ID
            coupon_name ,--优惠券名称
            coupon_amount,-- 使用的优惠券金额
            amount as pay_amount,--实付金额
            bonus,-- 使用的奖励金
            balance,--使用的余额
            opay_amount,--opay 支付的金额
            reference,-- opay 流水号
            currency,--货币类型
            country,--国家
            status,--支付模式（0: 支付中, 1: 成功, 2: 失败）
            modify_time,--最后修改时间
            create_time,--创建时间
            serv_type as product_id,--订单业务类型(0: all 1:driect 2: street)
            updated_at as updated_time,--最后更新时间
            pay_type --支付类型(0:手动支付 1:自动支付)

      FROM oride_dw_ods.ods_sqoop_base_data_order_payment_df
      WHERE dt='{pt}') pay
left outer join
(SELECT 
           order_id,
           city_id,--所属城市
            passenger_id,
             --乘客 ID
             part_hour,
             country_code,
             dt
      FROM oride_dw.dwd_oride_order_base_include_test_di
WHERE dt = '{pt}'
  and status=5
  AND city_id<>'999001' --去除测试数据
  ) ord
on pay.order_id=ord.order_id
'''.format(
        pt='{{ds}}',
        now_day='{{macros.ds_add(ds, +1)}}',
        table=table_name
        ),
schema='oride_dw',
    dag=dag)


#熔断数据，如果数据重复，报错
def check_key_data(ds,**kargs):

    #主键重复校验
    HQL_DQC='''
    SELECT count(1) as nm
    FROM
     (SELECT order_id,
             count(1) as cnt
      FROM oride_dw.{table}

      WHERE dt='{pt}'
      GROUP BY order_id HAVING count(1)>1) t1
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name
        )

    cursor = get_hive_cursor()
    logging.info('Executing 主键重复校验: %s', HQL_DQC)

    cursor.execute(HQL_DQC)
    res = cursor.fetchone()

    if res[0] >1:
        raise Exception ("Error The primary key repeat !", res)
    else:
        print("-----> Notice Data Export Success ......")
    
 
task_check_key_data = PythonOperator(
    task_id='check_data',
    python_callable=check_key_data,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 生成_SUCCESS ---------------------------------------## 

def fun_task_touchz_success(ds,**op_kwargs):

    #生成_SUCCESS
    ModelPublicFrame().task_touchz_success(table_list,ds)

task_touchz_success= PythonOperator(
    task_id='task_touchz_success',
    python_callable=fun_task_touchz_success,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 依赖关系配置 ---------------------------------------## 
for tasks_dependence in ModelPublicFrame().tesk_dependence(dependence_table_lists,dag):
    tasks_dependence>>dwd_oride_order_pay_detail_di_task>>sleep_time>>task_check_key_data>>task_touchz_success
