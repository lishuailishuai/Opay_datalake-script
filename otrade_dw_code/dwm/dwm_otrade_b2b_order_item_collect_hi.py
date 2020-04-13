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
    'start_date': datetime(2020, 4, 8),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_otrade_b2b_order_item_collect_hi',
                  schedule_interval="34 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dwm_otrade_b2b_order_item_collect_hi"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_category_hf
dwd_otrade_b2b_category_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2b_category_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_category_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwm_otrade_b2b_order_collect_hi
dwm_otrade_b2b_order_collect_hi_check_task = OssSensor(
    task_id='dwm_otrade_b2b_order_collect_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwm_otrade_b2b_order_collect_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_otrade_order_item_hi
dwd_otrade_b2b_otrade_order_item_hi_check_task = OssSensor(
    task_id='dwd_otrade_b2b_otrade_order_item_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_otrade_order_item_hi",
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


def dwm_otrade_b2b_order_item_collect_hi_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.取出商铺层级信息
with
category_info as (
  select
    *
  from
    otrade_dw.dwd_otrade_b2b_category_hf
  where 
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.种类层级表
category_level_info as (
  select
    v1.id as level3_id
    ,v1.name as level3_name
    ,v1.parent_id as level3_pid
    ,nvl(v2.id,0) as level2_id
    ,nvl(v2.name,'-') as level2_name
    ,nvl(v2.parent_id,0) as level2_pid
    ,nvl(v3.id,0) as level1_id
    ,nvl(v3.name,'-') as level1_name
    ,nvl(v3.parent_id,0) as level1_pid
  from
    (select * from category_info where level=3) v1
  left join
    category_info v2
  on
    v1.parent_id = v2.id
  left join
    category_info v3
  on
    v2.parent_id = v3.id
),

--3.取出截至目前最新的交易状态
order_info as (
  select
    *
  from
    (
    select
      *
      ,row_number() over(partition by order_id order by update_time desc) rn
    from
      otrade_dw.dwm_otrade_b2b_order_collect_hi as a
    where
      dt = substr(default.minLocalTimeRange("{config}", '{v_date}', 0),0,10)
      and hour >= '00'
      and hour <= substr(default.maxLocalTimeRange("{config}", '{v_date}', 0),12,2)
    ) as b
  where
    rn = 1
),

--4.取商品订单信息
order_item_info as (
  select
    id
    ,zorder_id
    ,order_id
    ,market_price
    ,sku_price
    ,buy_num
    ,total_amount
    ,product_id
    ,product_category_id
    ,product_title
    ,product_brand_id
    ,product_brand
    ,sku_id
    ,product_image
    ,refund_status
    ,sku_value
    ,zorder_status
    ,create_time
    ,handwork_fee
  from
    otrade_dw.dwd_otrade_b2b_otrade_order_item_hi
  where 
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--5.最后将去重的结果集插入到表中
insert overwrite table otrade_dw.dwm_otrade_b2b_order_item_collect_hi partition(country_code,dt,hour)
select
  v1.id
  ,v1.zorder_id
  ,v1.order_id
  ,nvl(v2.level1_id,0) as level1_id
  ,nvl(v2.level1_name,'-') as level1_name
  ,nvl(v2.level2_id,0) as level2_id
  ,nvl(v2.level2_name,'-') as level2_name
  ,v1.product_category_id as level3_id
  ,nvl(v2.level3_name,'-') as level3_name

  ,v1.product_id
  ,v1.product_title
  ,v1.sku_id
  ,v1.sku_value
  ,v1.product_brand_id
  ,v1.product_brand
  ,v1.market_price
  ,v1.sku_price
  ,v1.buy_num
  ,v1.total_amount
  ,v1.refund_status
  ,v1.zorder_status
  ,v1.create_time
  ,v1.handwork_fee

  ,v3.pay_id
  ,v3.settle_id
  ,v3.original_order_id
  ,v3.order_type
  ,v3.payer
  ,v3.payee
  ,v3.payer_phone
  ,v3.payee_phone
  ,v3.payer_name
  ,v3.payee_name
  ,v3.supplier_type
  ,v3.source
  ,v3.shop_id
  ,v3.shop_name
  ,v3.order_status
  ,v3.refund_status as order_refund_status
  ,v3.payable_amount
  ,v3.amount
  ,v3.fee_type
  ,v3.fee
  ,v3.fee_rate
  ,v3.pay_channel
  ,v3.pay_type
  ,v3.pay_cur
  ,v3.pay_time
  ,v3.consign_time
  ,v3.confirm_time
  ,v3.receive_time
  ,v3.refund_type
  ,v3.create_time as order_create_time
  ,v3.update_time as order_update_time
  ,v3.status
  ,v3.country
  ,v3.city
  ,v3.country_name
  ,v3.city_name
  ,v3.bd_invitation_code
  ,v3.bd_invitation_id
  ,v3.hcm_id
  ,v3.hcm_name
  ,v3.cm_id
  ,v3.cm_name
  ,v3.bdm_id
  ,v3.bdm_name
  ,v3.bd_id
  ,v3.bd_name
  ,v3.retailer_retailer_email
  ,v3.retailer_country
  ,v3.retailer_city
  ,v3.retailer_country_name
  ,v3.retailer_city_name
  ,v3.retailer_bd_invitation_id
  ,v3.retailer_hcm_id
  ,v3.retailer_hcm_name
  ,v3.retailer_cm_id
  ,v3.retailer_cm_name
  ,v3.retailer_bdm_id
  ,v3.retailer_bdm_name
  ,v3.retailer_bd_id
  ,v3.retailer_bd_name
  ,v3.retailer_first_order
  ,v3.retailer_first_order_time
  ,v3.opay_pay_id
  ,v3.actual_amount
  ,v3.pay_status
  ,v3.req_status

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour
  
  ,v3.supplier_create_time
  ,v3.retailer_create_time

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  order_item_info as v1
left join
  category_level_info as v2
on
  v1.product_category_id = v2.level3_id
left join
  order_info as v3
on
  v1.order_id = v3.order_id
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_otrade_b2b_order_item_collect_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwm_otrade_b2b_order_item_collect_hi_task = PythonOperator(
    task_id='dwm_otrade_b2b_order_item_collect_hi_task',
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

dwd_otrade_b2b_category_hf_check_task >> dwm_otrade_b2b_order_item_collect_hi_task
dwm_otrade_b2b_order_collect_hi_check_task >> dwm_otrade_b2b_order_item_collect_hi_task
dwd_otrade_b2b_otrade_order_item_hi_check_task >> dwm_otrade_b2b_order_item_collect_hi_task

