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

from plugins.CountriesAppFrame import CountriesAppFrame

args = {
    'owner': 'yuanfeng',
    'start_date': datetime(2020, 4, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_otrade_b2c_order_goods_collect_di',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dwm_otrade_b2c_order_goods_collect_di"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##

dwm_otrade_b2c_order_goods_collect_hi_task = OssSensor(
    task_id='dwm_otrade_b2c_order_goods_collect_hi_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwm_otrade_b2c_order_goods_collect_hi",
        pt='{{ds}}',
        hour='23'
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

def dwm_otrade_b2c_order_goods_collect_di_sql_task(ds):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.将数据关联后插入最终表中
insert overwrite table otrade_dw.dwm_otrade_b2c_order_goods_collect_di partition(country_code,dt,hour)
select
  id
  ,order_id
  ,goods_id
  ,goods_name
  ,goods_sn
  ,product_id
  ,number
  ,market_price
  ,retail_price
  ,goods_specifition_name_value
  ,is_real
  ,goods_specifition_ids
  ,list_pic_url
  ,brand_id
  ,customers
  ,customers_name
  ,country
  ,province
  ,city
  ,district
  ,address
  ,mobile
  ,consignee
  ,shipping_id
  ,shipping_name
  ,shipping_no
  ,shipping_status
  ,order_price
  ,goods_price
  ,actual_price
  ,coupon_id
  ,order_sn
  ,user_id
  ,order_status
  ,pay_status
  ,postscript
  ,pay_id
  ,pay_name
  ,shipping_fee
  ,integral
  ,integral_money
  ,add_time
  ,confirm_time
  ,pay_time
  ,freight_price
  ,parent_id
  ,coupon_price
  ,callback_status
  ,full_cut_price
  ,order_type
  ,settlement_total_fee
  ,all_price
  ,all_order_id
  ,promoter_id
  ,brokerage
  ,merchant_id
  ,group_buying_id
  ,user_mobile
  ,opayid
  ,merchant_name
  ,merchant_mobile
  ,merchant_account
  ,merchant_account_name
  ,merchant_create_date
  ,first_order
  ,first_order_time
  ,city_name
  ,country_name
  ,one_level_id
  ,one_level_name
  ,two_level_id
  ,two_level_name
  ,three_level_id
  ,three_level_name
  ,product_name
  ,product_retail_price
  ,product_market_price
  ,product_group_price

  ,'NG' as country_code
  ,'{pt}' as dt
from
  (
  select
    id
    ,order_id
    ,goods_id
    ,goods_name
    ,goods_sn
    ,product_id
    ,number
    ,market_price
    ,retail_price
    ,goods_specifition_name_value
    ,is_real
    ,goods_specifition_ids
    ,list_pic_url
    ,brand_id
    ,customers
    ,customers_name
    ,country
    ,province
    ,city
    ,district
    ,address
    ,mobile
    ,consignee
    ,shipping_id
    ,shipping_name
    ,shipping_no
    ,shipping_status
    ,order_price
    ,goods_price
    ,actual_price
    ,coupon_id
    ,order_sn
    ,user_id
    ,order_status
    ,pay_status
    ,postscript
    ,pay_id
    ,pay_name
    ,shipping_fee
    ,integral
    ,integral_money
    ,add_time
    ,confirm_time
    ,pay_time
    ,freight_price
    ,parent_id
    ,coupon_price
    ,callback_status
    ,full_cut_price
    ,order_type
    ,settlement_total_fee
    ,all_price
    ,all_order_id
    ,promoter_id
    ,brokerage
    ,merchant_id
    ,group_buying_id
    ,user_mobile
    ,opayid
    ,merchant_name
    ,merchant_mobile
    ,merchant_account
    ,merchant_account_name
    ,merchant_create_date
    ,first_order
    ,first_order_time
    ,city_name
    ,country_name
    ,one_level_id
    ,one_level_name
    ,two_level_id
    ,two_level_name
    ,three_level_id
    ,three_level_name
    ,product_name
    ,product_retail_price
    ,product_market_price
    ,product_group_price
    
    ,row_number() over(partition by id order by utc_date_hour desc) rn
  from
    otrade_dw.dwm_otrade_b2c_order_goods_collect_hi
  where
    dt = '{pt}'
  ) as a
where
  rn = 1
;



'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    # 读取sql
    # _sql = dwm_otrade_b2c_order_goods_collect_di_sql_task(ds)

    # logging.info('Executing: %s', _sql)

    # 执行Hive
    # hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    # TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "false")

    """
        #功能函数
            alter语句: alter_partition()
            删除分区: delete_partition()
            生产success: touchz_success()

        #参数
            is_countries_online --是否开通多国家业务 默认(true 开通)
            db_name --hive 数据库的名称
            table_name --hive 表的名称
            data_oss_path --oss 数据目录的地址
            is_country_partition --是否有国家码分区,[默认(true 有country_code分区)]
            is_result_force_exist --数据是否强行产出,[默认(true 必须有数据才生成_SUCCESS)] false 数据没有也生成_SUCCESS 
            execute_time --当前脚本执行时间(%Y-%m-%d %H:%M:%S)
            is_hour_task --是否开通小时级任务,[默认(false)]
            frame_type --模板类型(只有 is_hour_task:'true' 时生效): utc 产出分区为utc时间，local 产出分区为本地时间,[默认(utc)]。
            is_offset --是否开启时间前后偏移(影响success 文件)
            execute_time_offset --执行时间偏移值(-1、0、1),在当前执行时间上，前后偏移原有时间，用于产出前后小时分区
            business_key --产品线名称

        #读取sql
            %_sql(ds,v_hour)

    """

    args = [
        {
            "dag": dag,
            "is_countries_online": "true",
            "db_name": db_name,
            "table_name": table_name,
            "data_oss_path": hdfs_path,
            "is_country_partition": "true",
            "is_result_force_exist": "false",
            "execute_time": v_date,
            "is_hour_task": "false",
            "frame_type": "local",
            "is_offset": "true",
            "execute_time_offset": -1,
            "business_key": "otrade"
        }
    ]

    cf = CountriesAppFrame(args)

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_otrade_b2c_order_goods_collect_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwm_otrade_b2c_order_goods_collect_di_task = PythonOperator(
    task_id='dwm_otrade_b2c_order_goods_collect_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)

dwm_otrade_b2c_order_goods_collect_hi_task >> dwm_otrade_b2c_order_goods_collect_di_task





