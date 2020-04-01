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
    'start_date': datetime(2020, 3, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dim_otrade_b2b_retailer_info_crm_hf',
                  schedule_interval="30 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dim_otrade_b2b_retailer_info_crm_hf"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
### 检查最新的用户表的依赖
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

###oss://opay-datalake/otrade/otrade_dw/dim_otrade_b2b_bd_info_hf
dim_otrade_b2b_bd_info_hf_check_task = OssSensor(
    task_id='dim_otrade_b2b_bd_info_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dim_otrade_b2b_bd_info_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_otrade_business_details_hf
dwd_otrade_b2b_otrade_business_details_hf_check_task = OssSensor(
    task_id='dwd_otrade_b2b_otrade_business_details_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_otrade_business_details_hf",
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


def dim_otrade_b2b_retailer_info_crm_hf_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--0.取出城市编码和国家码
with
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

--1.bd信息
bd_info as (
  select
    id  
    ,job_id
    ,job_name
    ,hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_id
    ,bd_name
  from
    otrade_dw.dim_otrade_b2b_bd_info_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.首先取出上一个小时全量的商铺数据
retailer_info as (
  select 
    id
    ,opay_id
    ,opay_account
    ,first_name
    ,last_name
    ,other_name
    ,phone_number
    ,retailer_email
    ,address
    ,city_id
    ,retailer_gender
    ,retailer_bvn
    ,shop_category
    ,door_head_photos
    ,shop_owner_photos
    ,cashier_desk_photos
    ,store_furnishings_photos
    ,department_id
    ,lng
    ,lat
    ,retailer_status
    ,fence_id
    ,create_user_id
    ,bd_id
    ,created_at
    ,updated_at
    ,certification_time
  from
    otrade_dw.dwd_otrade_b2b_otrade_business_details_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--3.最后插入数据
insert overwrite table otrade_dw.dim_otrade_b2b_retailer_info_crm_hf partition(country_code,dt,hour)
select
  v1.id
  ,v1.opay_id
  ,v1.opay_account
  ,v1.first_name
  ,v1.last_name
  ,v1.other_name
  ,v1.phone_number
  ,v1.retailer_email
  ,v1.address

  ,v3.country_code as country
  ,v3.country_name
  ,v1.city_id
  ,v3.city_name

  ,v1.retailer_gender
  ,v1.retailer_bvn
  ,v1.shop_category
  ,v1.door_head_photos
  ,v1.shop_owner_photos
  ,v1.cashier_desk_photos
  ,v1.store_furnishings_photos
  ,v1.department_id
  ,v1.lng
  ,v1.lat
  ,v1.retailer_status
  ,v1.fence_id
  ,v1.create_user_id
  ,v1.bd_id

  ,v2.job_id
  ,v2.job_name
  ,v2.hcm_id
  ,v2.hcm_name
  ,v2.cm_id
  ,v2.cm_name
  ,v2.bdm_id
  ,v2.bdm_name
  ,v2.bd_id as bd_real_id
  ,v2.bd_name

  ,v1.created_at
  ,v1.updated_at
  ,v1.certification_time

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  retailer_info as v1 
left join
  bd_info as v2
on
  v1.bd_id = v2.id
left join
  city_info as v3
on
  v1.city_id = v3.city_code
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
    _sql = "\n" + cf.alter_partition() + "\n" + dim_otrade_b2b_retailer_info_crm_hf_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dim_otrade_b2b_retailer_info_crm_hf_task = PythonOperator(
    task_id='dim_otrade_b2b_retailer_info_crm_hf_task',
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

dim_otrade_b2b_city_info_hf_check_task >> dim_otrade_b2b_retailer_info_crm_hf_task
dim_otrade_b2b_bd_info_hf_check_task >> dim_otrade_b2b_retailer_info_crm_hf_task
dwd_otrade_b2b_otrade_business_details_hf_check_task >> dim_otrade_b2b_retailer_info_crm_hf_task


