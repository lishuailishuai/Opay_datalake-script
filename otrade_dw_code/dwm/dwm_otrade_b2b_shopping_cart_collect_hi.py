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
    'start_date': datetime(2020, 4, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_otrade_b2b_shopping_cart_collect_hi',
                  schedule_interval="34 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dwm_otrade_b2b_shopping_cart_collect_hi"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_product_shopping_cart_hi
dwd_otrade_b2b_product_shopping_cart_hi_check_task = OssSensor(
    task_id='dwd_otrade_b2b_product_shopping_cart_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_product_shopping_cart_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dim_otrade_b2b_goods_info_hf
dim_otrade_b2b_goods_info_hf_check_task = OssSensor(
    task_id='dim_otrade_b2b_goods_info_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dim_otrade_b2b_goods_info_hf",
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

###oss://opay-datalake/otrade/otrade_dw/dim_otrade_b2b_retailer_info_crm_hf
dim_otrade_b2b_retailer_info_crm_hf_check_task = OssSensor(
    task_id='dim_otrade_b2b_retailer_info_crm_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dim_otrade_b2b_retailer_info_crm_hf",
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


def dwm_otrade_b2b_shopping_cart_collect_hi_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.购物车信息
with 
cart_info as (
  select
    id
    ,user_id
    ,sku_id
    ,buy_num
    ,create_time
    ,update_time
  from
    otrade_dw.dwd_otrade_b2b_product_shopping_cart_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.物品信息
goods_info as (
  select
    level1_id
    ,level1_name
    ,level2_id
    ,level2_name
    ,level3_id
    ,level3_name
    ,spu_id
    ,spu_name
    ,sku_id
    ,sku_name
    ,spu_brand_id
    ,spu_brand
    ,spu_supplier_id
    ,spu_supplier_name
    ,spu_shop_id
    ,sku_price
  from
    otrade_dw.dim_otrade_b2b_goods_info_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--3.供应商信息
supplier_info as (
  select
    id
    ,opay_id
    ,country
    ,city
    ,country_name
    ,city_name
    ,job_id
    ,job_name
    ,hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_real_id
    ,bd_name
  from
    otrade_dw.dim_otrade_b2b_supplier_info_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--4.零售商信息
retailer_info as (
  select
    id
    ,opay_id
    ,first_name
    ,country
    ,country_name
    ,city_id
    ,city_name
    ,job_id
    ,job_name
    ,hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_real_id
    ,bd_name
  from
    otrade_dw.dim_otrade_b2b_retailer_info_crm_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--5.最后插入数据到表中
insert overwrite table otrade_dw.dwm_otrade_b2b_shopping_cart_collect_hi partition(country_code,dt,hour)
select
  v1.id
  ,v1.user_id
  ,v1.buy_num
  ,v1.create_time
  ,v1.update_time

  ,v2.level1_id
  ,v2.level1_name
  ,v2.level2_id
  ,v2.level2_name
  ,v2.level3_id
  ,v2.level3_name
  ,v2.spu_id
  ,v2.spu_name
  ,v1.sku_id
  ,v2.sku_name
  ,v2.spu_brand_id
  ,v2.spu_brand

  ,v2.spu_supplier_id as supplier_opay_id
  ,v2.spu_supplier_name  as supplier_name
  ,v2.spu_shop_id as supplier_shop_id
  ,v3.country as supplier_country
  ,v3.city as supplier_city
  ,v3.country_name as supplier_country_name
  ,v3.city_name as supplier_city_name
  ,v3.job_id as supplier_job_id
  ,v3.job_name as supplier_job_name
  ,v3.hcm_id as supplier_hcm_id
  ,v3.hcm_name as supplier_hcm_name
  ,v3.cm_id as supplier_cm_id
  ,v3.cm_name as supplier_cm_name
  ,v3.bdm_id as supplier_bdm_id
  ,v3.bdm_name as supplier_bdm_name
  ,v3.bd_real_id as supplier_bd_id
  ,v3.bd_name as supplier_bd_name

  ,v1.user_id as retailer_opay_id
  ,v4.first_name as retailer_name
  ,v4.id as retailer_shop_id
  ,v4.country as retailer_country
  ,v4.city_id as retailer_city
  ,v4.country_name as retailer_country_name
  ,v4.city_name as retailer_city_name
  ,v4.job_id as retailer_job_id
  ,v4.job_name as retailer_job_name
  ,v4.hcm_id as retailer_hcm_id
  ,v4.hcm_name as retailer_hcm_name
  ,v4.cm_id as retailer_cm_id
  ,v4.cm_name as retailer_cm_name
  ,v4.bdm_id as retailer_bdm_id
  ,v4.bdm_name as retailer_bdm_name
  ,v4.bd_real_id as retailer_bd_id
  ,v4.bd_name as retailer_bd_name

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  cart_info as v1
left join
  goods_info as v2
on
  v1.sku_id = v2.sku_id
left join
  supplier_info as v3
on
  v2.spu_shop_id = v3.id
left join
  retailer_info as v4
on
  v1.user_id = v4.opay_id
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_otrade_b2b_shopping_cart_collect_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwm_otrade_b2b_shopping_cart_collect_hi_task = PythonOperator(
    task_id='dwm_otrade_b2b_shopping_cart_collect_hi_task',
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

dwd_otrade_b2b_product_shopping_cart_hi_check_task >> dwm_otrade_b2b_shopping_cart_collect_hi_task
dim_otrade_b2b_goods_info_hf_check_task >> dwm_otrade_b2b_shopping_cart_collect_hi_task
dim_otrade_b2b_supplier_info_hf_check_task >> dwm_otrade_b2b_shopping_cart_collect_hi_task
dim_otrade_b2b_retailer_info_crm_hf_check_task >> dwm_otrade_b2b_shopping_cart_collect_hi_task

