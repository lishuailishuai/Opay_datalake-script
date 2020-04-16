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
    'start_date': datetime(2020, 4, 16),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oexpress_order_collect_hi',
                  schedule_interval="42 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "oexpress_dw"
table_name = "dwm_oexpress_order_collect_hi"
hdfs_path = "oss://opay-datalake/oexpress/oexpress_dw/" + table_name
config = eval(Variable.get("oexpress_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
###oss://opay-datalake/oexpress/oexpress_dw/dim_oexpress_hub_info_hf
dim_oexpress_hub_info_hf_check_task = OssSensor(
    task_id='dim_oexpress_hub_info_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oexpress/oexpress_dw/dim_oexpress_hub_info_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/oexpress/oexpress_dw/dwd_oexpress_data_order_hi
dwd_oexpress_data_order_hi_check_task = OssSensor(
    task_id='dwd_oexpress_data_order_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oexpress/oexpress_dw/dwd_oexpress_data_order_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/oexpress/oexpress_dw/dwd_oexpress_data_transport_order_hi
dwd_oexpress_data_transport_order_hi_check_task = OssSensor(
    task_id='dwd_oexpress_data_transport_order_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oexpress/oexpress_dw/dwd_oexpress_data_transport_order_hi",
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

    v_date = GetLocalTime("oexpress", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['date']
    v_hour = GetLocalTime("oexpress", execution_date.strftime("%Y-%m-%d %H"), v_country_code, v_gap_hour)['hour']

    # 小时级监控
    tb_hour_task = [
        {"dag": dag, "db": "oexpress_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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


def dwm_oexpress_order_collect_hi_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.取仓库信息
with
hub_info as (
  select
    id
    ,country_id
    ,city_id
    ,name
    ,address
    ,lat
    ,lng
    ,contact_person
    ,contact_phone
    ,status
    ,created_at
    ,updated_at
    ,business_hours
    ,city_name
    ,country_name
    ,pick_up_area
    ,deliver_area
  from
    oexpress_dw.dim_oexpress_hub_info_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.订单信息
order_info as (
  select
    *
  from
    oexpress_dw.dwd_oexpress_data_order_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--3.配送信息
transport_info as (
  select
    *
  from
    oexpress_dw.dwm_oexpress_transport_collect_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--4.将信息关联后插入原表
insert overwrite table oexpress_dw.dwm_oexpress_order_collect_hi partition(country_code,dt,hour)
select
  v1.id
  ,v1.city_id
  ,v1.create_user_id
  ,v1.order_source
  ,v1.sender_cell
  ,v1.sender_first_name
  ,v1.sender_last_name
  ,v1.without_collect
  ,v1.ori_hub_id
  ,v1.ori_lat
  ,v1.ori_lng
  ,v1.ori_addr
  ,v1.ori_detailed_addr
  
  ,v3.name as ori_name
  ,v3.country_id as ori_country_id
  ,v3.country_name as ori_country_name
  ,v3.city_id as ori_city_id
  ,v3.city_name as ori_city_name
  ,v3.contact_person as ori_contact_person
  ,v3.contact_phone as ori_contact_phone
  ,v3.status as ori_status
  ,v3.created_at as ori_created_at
  ,v3.business_hours as ori_business_hours
  
  ,v1.receiver_cell
  ,v1.receiver_first_name
  ,v1.receiver_last_name
  ,v1.dest_hub_id
  ,v1.dest_lat
  ,v1.dest_lng
  ,v1.dest_addr
  ,v1.dest_detailed_addr
  
  ,v4.name as dest_name
  ,v4.country_id as dest_country_id
  ,v4.country_name as dest_country_name
  ,v4.city_id as dest_city_id
  ,v4.city_name as dest_city_name
  ,v4.contact_person as dest_contact_person
  ,v4.contact_phone as dest_contact_phone
  ,v4.status as dest_status
  ,v4.created_at as dest_created_at
  ,v4.business_hours as dest_business_hours
  
  ,v1.current_transport_id
  ,v1.current_hold_record_id
  ,v1.status
  ,v1.confirm_time
  ,v1.collected_time
  ,v1.finish_time
  ,v1.close_time
  ,v1.cancel_time
  ,v1.cancel_role
  ,v1.cancel_comment
  ,v1.product_category
  ,v1.product_category_name
  ,v1.basic_fee
  ,v1.weight_fee
  ,v1.insurance_fee
  ,v1.pickup_fee
  ,v1.tax_fee
  ,v1.deliver_fee
  ,v1.payment_method
  ,v1.price
  ,v1.weight
  ,v1.volume
  ,v1.comment
  ,v1.delivery_code
  ,v1.pickup_pic_url_list
  ,v1.delivered_pic_url_list
  ,v1.create_time
  ,v1.update_time
  ,v1.item_code
  ,v1.cash_received
  ,v1.use_universal_code
  
  ,v2.order_id
  ,v2.transport_type
  ,v2.status as transport_status
  ,v2.driver_id
  ,v2.driver_name
  ,v2.driver_phone_number
  ,v2.driver_serv_type
  ,v2.driver_identity_type
  ,v2.driver_hub_id
  ,v2.driver_working_status
  ,v2.driver_identity_status
  ,v2.driver_vehicle_id
  ,v2.driver_plate_number
  ,v2.driver_country_id
  ,v2.driver_country_name
  ,v2.driver_city_id
  ,v2.driver_city_name
  ,v2.driver_first_bind_time
  ,v2.driver_created_at
  ,v2.driver_hub_name
  ,v2.driver_hub_address
  ,v2.driver_hub_lat
  ,v2.driver_hub_lng
  ,v2.driver_hub_contact_person
  ,v2.driver_hub_contact_phone
  ,v2.driver_hub_status
  ,v2.create_time as transport_create_time
  ,v2.update_time as transport_update_time
  ,v2.assigned_time
  ,v2.collect_status
  ,v2.collect_time
  ,v2.delivered_time
  ,v2.closed_time
  ,v2.reassign_time
  ,v2.reassign_src_transport_id
  ,v2.display_type
  ,v2.sequence_idx
  ,v2.estimated_distance

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  order_info as v1
left join
  transport_info as v2
on
  v1.current_transport_id = v2.id
left join
  hub_info as v3
on
  v1.ori_hub_id = v3.id
left join
  hub_info as v4
on
  v1.dest_hub_id = v4.id
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_oexpress_order_collect_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwm_oexpress_order_collect_hi_task = PythonOperator(
    task_id='dwm_oexpress_order_collect_hi_task',
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

dim_oexpress_hub_info_hf_check_task >> dwm_oexpress_order_collect_hi_task
dwd_oexpress_data_order_hi_check_task >> dwm_oexpress_order_collect_hi_task
dwd_oexpress_data_transport_order_hi_check_task >> dwm_oexpress_order_collect_hi_task


