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
    'start_date': datetime(2020, 4, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_otrade_b2c_order_collect_hi',
                  schedule_interval="28 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dwd_otrade_b2c_order_collect_hi"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2c_mall_merchant_hf
dwd_otrade_b2c_mall_merchant_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2c_mall_merchant_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2c_mall_merchant_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwm_otrade_b2c_user_first_hf
dwm_otrade_b2c_user_first_hf_check_task = OssSensor(
    task_id='dwm_otrade_b2c_user_first_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwm_otrade_b2c_user_first_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_order_hi
dwd_otrade_b2c_mall_nideshop_order_hi_check_task = OssSensor(
    task_id='dwd_otrade_b2c_mall_nideshop_order_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_order_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dim_otrade_b2b_city_info_hf
dim_otrade_b2b_city_info_hf_check_task = OssSensor(
    task_id='dim_otrade_b2b_city_info_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dim_otrade_b2b_city_info_hf",
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


def dwd_otrade_b2c_order_collect_hi_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.先取得供应商信息
with 
merchant_info as (
  select
    id
    ,shop_name
    ,mobile
    ,account
    ,account_name
    ,create_time
  from
    otrade_dw.dwd_otrade_b2c_mall_merchant_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.再取得付款方信息
user_first_info as (
  select
    user_opayid
    ,first_order_time
  from
    otrade_dw.dwm_otrade_b2c_user_first_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--3.城市相关
city_info as (
  select
    city_code
    ,city_name
    ,country_code
    ,country_name
  from
    otrade_dw.dim_otrade_b2b_city_info_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--4.订单相关
order_info as (
  select
    id
    ,order_sn
    ,user_id
    ,order_status
    ,shipping_status
    ,pay_status
    ,consignee
    ,country
    ,province
    ,city
    ,district
    ,address
    ,mobile
    ,postscript
    ,shipping_id
    ,shipping_name
    ,pay_id
    ,pay_name
    ,shipping_fee
    ,actual_price
    ,integral
    ,integral_money
    ,order_price
    ,goods_price
    ,add_time
    ,confirm_time
    ,pay_time
    ,freight_price
    ,coupon_id
    ,parent_id
    ,coupon_price
    ,callback_status
    ,shipping_no
    ,full_cut_price
    ,order_type
    ,brand_id
    ,settlement_total_fee
    ,all_price
    ,all_order_id
    ,promoter_id
    ,brokerage
    ,merchant_id
    ,group_buying_id
    ,user_mobile
    ,opayid
  from
    otrade_dw.dwd_otrade_b2c_mall_nideshop_order_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--5.将数据关联后插入最终表中
insert overwrite table otrade_dw.dwd_otrade_b2c_order_collect_hi partition(country_code,dt,hour)
select
  v1.id
  ,v1.order_sn
  ,v1.user_id
  ,v1.order_status
  ,v1.shipping_status
  ,v1.pay_status
  ,v1.consignee
  ,v1.country
  ,v1.province
  ,v1.city
  ,v1.district
  ,v1.address
  ,v1.mobile
  ,v1.postscript
  ,v1.shipping_id
  ,v1.shipping_name
  ,v1.pay_id
  ,v1.pay_name
  ,v1.shipping_fee
  ,v1.actual_price
  ,v1.integral
  ,v1.integral_money
  ,v1.order_price
  ,v1.goods_price
  ,v1.add_time
  ,v1.confirm_time
  ,v1.pay_time
  ,v1.freight_price
  ,v1.coupon_id
  ,v1.parent_id
  ,v1.coupon_price
  ,v1.callback_status
  ,v1.shipping_no
  ,v1.full_cut_price
  ,v1.order_type
  ,v1.brand_id
  ,v1.settlement_total_fee
  ,v1.all_price
  ,v1.all_order_id
  ,v1.promoter_id
  ,v1.brokerage
  ,v1.merchant_id
  ,v1.group_buying_id
  ,v1.user_mobile
  ,v1.opayid

  --商铺相关
  ,v2.shop_name as merchant_name
  ,v2.mobile as merchant_mobile
  ,v2.account as merchant_account
  ,v2.account_name as merchant_account_name
  ,nvl(substr(v2.create_time,0,10),'substr(v1.add_time,0,10)') as merchant_create_date

  --用户相关
  ,if(v1.add_time > v3.first_order_time,0,1) as first_order
  ,v3.first_order_time as first_order_time

  --城市国家名称
  ,v4.city_name
  ,v4.country_name

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  order_info as v1
left join
  merchant_info as v2
on
  v1.merchant_id = v2.id
left join
  user_first_info as v3
on
  v1.opayid = v3.user_opayid
left join
  city_info as v4
on
  v1.city = v4.city_code
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_otrade_b2c_order_collect_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_otrade_b2c_order_collect_hi_task = PythonOperator(
    task_id='dwd_otrade_b2c_order_collect_hi_task',
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

dwd_otrade_b2c_mall_merchant_hf_check_task >> dwd_otrade_b2c_order_collect_hi_task
dwm_otrade_b2c_user_first_hf_check_task >> dwd_otrade_b2c_order_collect_hi_task
dwd_otrade_b2c_mall_nideshop_order_hi_check_task >> dwd_otrade_b2c_order_collect_hi_task
dim_otrade_b2b_city_info_hf_check_task >> dwd_otrade_b2c_order_collect_hi_task

