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
    'start_date': datetime(2020, 3, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_otrade_b2c_mall_nideshop_product_hf',
                  schedule_interval="28 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dim_otrade_b2c_mall_nideshop_product_hf"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
### 检查最新的用户表的依赖
###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_category_hf
dwd_otrade_b2c_mall_nideshop_category_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2c_mall_nideshop_category_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_category_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查最新的用户表的依赖
###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_product_hf
dwd_otrade_b2c_mall_nideshop_product_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2c_mall_nideshop_product_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_product_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查最新的用户表的依赖
###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_goods_hf
dwd_otrade_b2c_mall_nideshop_goods_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2c_mall_nideshop_goods_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2c_mall_nideshop_goods_hf",
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


def dim_otrade_b2c_mall_nideshop_product_hf_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.取出上一个小时的全量
with
category_info as (
  select
    id
    ,name
    ,parent_id
    ,level
  from
    otrade_dw.dwd_otrade_b2c_mall_nideshop_category_hf
  where 
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.种类层级表
category_level_info as (
  select
    v4.id as one_level_id
    ,v4.name as one_level_name
    ,v4.parent_id as one_level_parent_id
    ,nvl(v3.r_id,0) as two_level_id
    ,nvl(v3.r_name,'-') as two_level_name
    ,v3.r_parent_id as two_parent_id
    ,nvl(v3.l_id,0) as three_level_id
    ,nvl(v3.l_name,'-') as three_level_name
    ,v3.l_parent_id as three_parent_id
  from    
    (
    select
      v1.id as l_id
      ,v1.name as l_name
      ,v1.parent_id as l_parent_id
  
      ,nvl(v2.id,0) as r_id
      ,nvl(v2.name,'-') as r_name
      ,v2.parent_id as r_parent_id
    from
      category_info as v1
    left join
      category_info as v2
    on
      v1.parent_id = v2.id
    ) as v3
  left join
    category_info as v4
  on
    v3.r_parent_id = v4.id
),

--3.sku信息
sku_info as (
  select
    id as product_id
    ,goods_id
    ,retail_price as product_retail_price
    ,market_price as product_market_price
    ,group_price as product_group_price
    ,goods_specification_name as product_name
    ,merchant_id as product_merchant_id
  from
    otrade_dw.dwd_otrade_b2c_mall_nideshop_product_hf
  where
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--4.spu信息
spu_info as (
  select
    id as goods_id
    ,category_id
    ,name as goods_name
    ,is_on_sale as goods_is_on_sale
    ,is_delete as goods_is_delete
    ,is_new as goods_is_new
    ,is_secKill as goods_is_secKill
    ,is_service as goods_is_service
    ,is_app_exclusive as goods_is_app_exclusive
    ,is_limited as goods_is_limited
    ,is_hot as goods_is_hot
    ,brokerage_percent as goods_brokerage_percent
    ,merchant_id as goods_merchant_id
    ,start_time as goods_start_time
    ,end_time as goods_end_time
    ,add_time as goods_add_time
  from
    otrade_dw.dwd_otrade_b2c_mall_nideshop_goods_hf
  where
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--3.最后插入数据到表中
insert overwrite table otrade_dw.dim_otrade_b2c_mall_nideshop_product_hf partition(country_code,dt,hour)
select
  nvl(v3.one_level_id,0) as one_level_id
  ,nvl(v3.one_level_name,'-') as one_level_name
  ,nvl(v3.two_level_id,0) as two_level_id
  ,nvl(v3.two_level_name,'-') as two_level_name
  ,nvl(v3.three_level_id,0) as three_level_id
  ,nvl(v3.three_level_name,'-') as three_level_name

  ,nvl(v2.goods_id,0) as goods_id
  ,nvl(v2.goods_name,'-') as goods_name
  ,v1.product_id
  ,v1.product_name
  ,v1.product_retail_price
  ,v1.product_market_price
  ,v1.product_group_price
  ,v1.product_merchant_id

  ,v2.goods_is_on_sale
  ,v2.goods_is_delete
  ,v2.goods_is_new
  ,v2.goods_is_seckill
  ,v2.goods_is_service
  ,v2.goods_is_app_exclusive
  ,v2.goods_is_limited
  ,v2.goods_is_hot
  ,v2.goods_brokerage_percent
  ,v2.goods_merchant_id
  ,v2.goods_start_time
  ,v2.goods_end_time
  ,v2.goods_add_time

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  sku_info as v1
left join
  spu_info as v2
on
  v1.goods_id = v2.goods_id
left join
  category_level_info as v3
on
  v2.category_id = v3.three_level_id
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
    _sql = "\n" + cf.alter_partition() + "\n" + dim_otrade_b2c_mall_nideshop_product_hf_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dim_otrade_b2c_mall_nideshop_product_hf_task = PythonOperator(
    task_id='dim_otrade_b2c_mall_nideshop_product_hf_task',
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

dwd_otrade_b2c_mall_nideshop_category_hf_check_task >> dim_otrade_b2c_mall_nideshop_product_hf_task
dwd_otrade_b2c_mall_nideshop_product_hf_check_task >> dim_otrade_b2c_mall_nideshop_product_hf_task
dwd_otrade_b2c_mall_nideshop_goods_hf_check_task >> dim_otrade_b2c_mall_nideshop_product_hf_task


