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

dag = airflow.DAG('dim_otrade_b2b_goods_info_hf',
                  schedule_interval="28 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dim_otrade_b2b_goods_info_hf"
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

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_product_hf
dwd_otrade_b2b_product_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2b_product_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_product_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_product_sku_hf
dwd_otrade_b2b_product_sku_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2b_product_sku_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_product_sku_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_product_sku_property_hf
dwd_otrade_b2b_product_sku_property_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2b_product_sku_property_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_product_sku_property_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dim_otrade_b2b_supplier_info_hf
dim_otrade_b2b_supplier_info_hf_check_task = OssSensor(
    task_id='dim_otrade_b2b_supplier_info_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dim_otrade_b2b_supplier_info_hf",
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
def fun_task_timeout_monitor(ds,dag,execution_date,**op_kwargs):

    dag_ids=dag.dag_id

    #监控国家
    v_country_code='NG'

    #时间偏移量
    v_gap_hour=0

    v_date=GetLocalTime("otrade",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['date']
    v_hour=GetLocalTime("otrade",execution_date.strftime("%Y-%m-%d %H"),v_country_code,v_gap_hour)['hour']

    #小时级监控
    tb_hour_task = [
        {"dag":dag,"db": "otrade_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code={country_code}/dt={pt}/hour={now_hour}".format(country_code=v_country_code,pt=v_date,now_hour=v_hour), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb_hour_task)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

def dim_otrade_b2b_goods_info_hf_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.取出上一个小时的全量
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

--3.spu信息
spu_info as (
  select
    id as spu_id
    ,title as spu_name
    ,category_id
    ,brand_id as spu_brand_id
    ,brand as spu_brand
    ,supplier_id as spu_supplier_id
    ,supplier_name as spu_supplier_name
    ,shop_id as spu_shop_id
    ,original_price as spu_original_price
    ,price_min as spu_price_min
    ,price_max as spu_price_max
    ,stock as spu_stock
    ,starting_quantity as spu_starting_quantity
    ,status as spu_status
    ,create_time as spu_create_time
  from
    otrade_dw.dwd_otrade_b2b_product_hf
  where
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--4.sku信息
sku_info as (
  select
    id as sku_id
    ,product_id
    ,price as sku_price
    ,stock as sku_stock
    ,sales as sku_sales
    ,create_time as sku_create_time
  from
    otrade_dw.dwd_otrade_b2b_product_sku_hf
  where
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--5.sku参数信息
sku_property_info as (
  select 
    sku_id
    ,cast(concat_ws(',',collect_list(concat(property,':',value))) as string) as sku_name 
  from
    otrade_dw.dwd_otrade_b2b_product_sku_property_hf
  where
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
  group by 
    sku_id
),

--6.sku参数信息有脏数据,将sku做关联,取出信息
sku_join_info as (
  select
    v1.sku_id
    ,v1.product_id
    ,v1.sku_price
    ,v1.sku_stock
    ,v1.sku_sales
    ,v1.sku_create_time
    ,v2.sku_name
  from
    sku_info as v1
  left join
    sku_property_info as v2
  on
    v1.sku_id = v2.sku_id
),

--7.取出商铺bd信息
shop_info as (
  select
    id
    ,country
    ,country_name
    ,city
    ,city_name
    ,hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_real_id as bd_id
    ,bd_name
  from
    otrade_dw.dim_otrade_b2b_supplier_info_hf
  where
    concat(dt, " ", hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0) 
    and concat(dt, " ", hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
  group by
    id
    ,country
    ,country_name
    ,city
    ,city_name
    ,hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_real_id
    ,bd_name
)

--6.最后插入数据到表中
insert overwrite table otrade_dw.dim_otrade_b2b_goods_info_hf partition(country_code,dt,hour)
select
  v3.level1_id
  ,v3.level1_name
  ,v3.level2_id
  ,v3.level2_name
  ,v3.level3_id
  ,v3.level3_name

  ,v2.spu_id
  ,v2.spu_name
  ,v1.sku_id
  ,v1.sku_name
  ,v2.spu_brand_id
  ,v2.spu_brand
  ,v2.spu_supplier_id
  ,v2.spu_supplier_name
  ,v2.spu_shop_id
  ,v2.spu_original_price
  ,v2.spu_price_min
  ,v2.spu_price_max
  ,v2.spu_stock
  ,v2.spu_starting_quantity
  ,v2.spu_status
  ,v2.spu_create_time

  ,v1.sku_price
  ,v1.sku_stock
  ,v1.sku_sales
  ,v1.sku_create_time

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour
  
  ,v4.country
  ,v4.country_name
  ,v4.city
  ,v4.city_name
  ,v4.hcm_id
  ,v4.hcm_name
  ,v4.cm_id
  ,v4.cm_name
  ,v4.bdm_id
  ,v4.bdm_name
  ,v4.bd_id
  ,v4.bd_name

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  sku_join_info as v1
left join
  spu_info as v2
on
  v1.product_id = v2.spu_id
left join
  category_level_info as v3
on
  v2.category_id = v3.level3_id
left join
  shop_info as v4
on
  v2.spu_shop_id = v4.id
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
    _sql = "\n" + cf.alter_partition() + "\n" + dim_otrade_b2b_goods_info_hf_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dim_otrade_b2b_goods_info_hf_task = PythonOperator(
    task_id='dim_otrade_b2b_goods_info_hf_task',
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

dwd_otrade_b2b_category_hf_check_task >> dim_otrade_b2b_goods_info_hf_task
dwd_otrade_b2b_product_hf_check_task >> dim_otrade_b2b_goods_info_hf_task
dwd_otrade_b2b_product_sku_hf_check_task >> dim_otrade_b2b_goods_info_hf_task
dwd_otrade_b2b_product_sku_property_hf_check_task >> dim_otrade_b2b_goods_info_hf_task
dim_otrade_b2b_supplier_info_hf_check_task >> dim_otrade_b2b_goods_info_hf_task
