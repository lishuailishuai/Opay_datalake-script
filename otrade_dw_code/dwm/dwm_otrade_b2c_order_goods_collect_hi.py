# -*- coding: utf-8 -*-
import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.named_hive_partition_sensor import NamedHivePartitionSensor
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from airflow.sensors import UFileSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from airflow.sensors import OssSensor
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev

from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
from utils.get_local_time import GetLocalTime

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

dag = airflow.DAG('dwm_otrade_b2c_order_goods_collect_hi',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dwm_otrade_b2c_order_goods_collect_hi"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_order_goods_hi
dwd_otrade_b2c_mall_nideshop_order_goods_hi_check_task = OssSensor(
    task_id='dwd_otrade_b2c_mall_nideshop_order_goods_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_order_goods_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2c_order_collect_hi
dwd_otrade_b2c_order_collect_hi_check_task = OssSensor(
    task_id='dwd_otrade_b2c_order_collect_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2c_order_collect_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dim_otrade_b2c_mall_nideshop_product_hf
dim_otrade_b2c_mall_nideshop_product_hf_check_task = OssSensor(
    task_id='dim_otrade_b2c_mall_nideshop_product_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dim_otrade_b2c_mall_nideshop_product_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##
def fun_task_timeout_monitor(ds, dag, execution_date, **op_kwargs):
    dag_ids = dag.dag_id

    # 监控国家
    v_country_code = 'NG'

    # 时间偏移量
    v_gap_hour = 0

    v_date = GetLocalTime("otrade", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['date']
    v_hour = GetLocalTime("otrade", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['hour']

    # 小时级监控
    tb_hour_task = [
        {"dag": dag, "db": "otrade_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code={country_code}/dt={pt}/hour={now_hour}".format(country_code=v_country_code,
                                                                                   pt=v_date, now_hour=v_hour),
         "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


def dwm_otrade_b2c_order_goods_collect_hi_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.商品订单相关
with 
order_goods_info as (
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
  from
    otrade_dw.dwd_otrade_b2c_mall_nideshop_order_goods_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.订单相关
order_collect_info as (
  select
    id
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
  from
    otrade_dw.dwd_otrade_b2c_order_collect_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--3.商品层级相关
product_info as (
  select
    product_id
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
  from
    otrade_dw.dim_otrade_b2c_mall_nideshop_product_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--4.将数据关联后插入最终表中
insert overwrite table otrade_dw.dwm_otrade_b2c_order_goods_collect_hi partition(country_code,dt,hour)
select
  v1.id
  ,v1.order_id
  ,v1.goods_id
  ,v1.goods_name
  ,v1.goods_sn
  ,v1.product_id
  ,v1.number
  ,v1.market_price
  ,v1.retail_price
  ,v1.goods_specifition_name_value
  ,v1.is_real
  ,v1.goods_specifition_ids
  ,v1.list_pic_url
  ,v1.brand_id
  ,v1.customers
  ,v1.customers_name
  ,v1.country
  ,v1.province
  ,v1.city
  ,v1.district
  ,v1.address
  ,v1.mobile
  ,v1.consignee
  ,v1.shipping_id
  ,v1.shipping_name
  ,v1.shipping_no
  ,v1.shipping_status
  ,v1.order_price
  ,v1.goods_price
  ,v1.actual_price
  ,v1.coupon_id

  --以下为订单相关
  ,v2.order_sn
  ,v2.user_id
  ,v2.order_status
  ,v2.pay_status
  ,v2.postscript
  ,v2.pay_id
  ,v2.pay_name
  ,v2.shipping_fee
  ,v2.integral
  ,v2.integral_money
  ,v2.add_time
  ,v2.confirm_time
  ,v2.pay_time
  ,v2.freight_price
  ,v2.parent_id
  ,v2.coupon_price
  ,v2.callback_status
  ,v2.full_cut_price
  ,v2.order_type
  ,v2.settlement_total_fee
  ,v2.all_price
  ,v2.all_order_id
  ,v2.promoter_id
  ,v2.brokerage
  ,v2.merchant_id
  ,v2.group_buying_id
  ,v2.user_mobile
  ,v2.opayid
  ,v2.merchant_name
  ,v2.merchant_mobile
  ,v2.merchant_account
  ,v2.merchant_account_name
  ,v2.merchant_create_date
  ,v2.first_order
  ,v2.first_order_time
  ,v2.city_name
  ,v2.country_name

  --商品相关
  ,v3.one_level_id
  ,v3.one_level_name
  ,v3.two_level_id
  ,v3.two_level_name
  ,v3.three_level_id
  ,v3.three_level_name
  ,v3.product_name
  ,v3.product_retail_price
  ,v3.product_market_price
  ,v3.product_group_price

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  order_goods_info as v1
left join
  order_collect_info as v2
on
  v1.order_id = v2.id
left join
  product_info as v3
on
  v1.product_id = v3.product_id
;


    '''.format(
        pt=ds,
        v_date=v_date,
        table=table_name,
        db=db_name,
        config=config

    )
    return HQL


# 主流程
def execution_data_task_id(ds, dag, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

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
            "is_hour_task": "true",
            "frame_type": "local"
        }
    ]

    cf = CountriesPublicFrame_dev(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_otrade_b2c_order_goods_collect_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwm_otrade_b2c_order_goods_collect_hi_task = PythonOperator(
    task_id='dwm_otrade_b2c_order_goods_collect_hi_task',
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

dwd_otrade_b2c_mall_nideshop_order_goods_hi_check_task >> dwm_otrade_b2c_order_goods_collect_hi_task
dwd_otrade_b2c_order_collect_hi_check_task >> dwm_otrade_b2c_order_goods_collect_hi_task
dim_otrade_b2c_mall_nideshop_product_hf_check_task >> dwm_otrade_b2c_order_goods_collect_hi_task

