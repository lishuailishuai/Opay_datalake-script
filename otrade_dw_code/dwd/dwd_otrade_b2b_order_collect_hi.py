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
    'start_date': datetime(2020, 4, 2),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_otrade_b2b_order_collect_hi',
                  schedule_interval="26 * * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "dwd_otrade_b2b_order_collect_hi"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##
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

###oss://opay-datalake/otrade/otrade_dw/dwm_otrade_b2b_retailer_crm_first_hf
dwm_otrade_b2b_retailer_crm_first_hf_check_task = OssSensor(
    task_id='dwm_otrade_b2b_retailer_crm_first_hf_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwm_otrade_b2b_retailer_crm_first_hf",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_otrade_pay_hi
dwd_otrade_b2b_otrade_pay_hi_check_task = OssSensor(
    task_id='dwd_otrade_b2b_otrade_pay_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_otrade_pay_hi",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

###oss://opay-datalake/otrade/otrade_dw/dwd_otrade_b2b_otrade_order_hi
dwd_otrade_b2b_otrade_order_hi_check_task = OssSensor(
    task_id='dwd_otrade_b2b_otrade_order_hi_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_otrade_order_hi",
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


def dwd_otrade_b2b_order_collect_hi_sql_task(ds, v_date):
    HQL = '''

set mapred.max.split.size=1000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--1.先取得供应商信息
with 
supplier_info as (
  select
    id
    ,opay_id
    ,status
    ,country
    ,city
    ,country_name
    ,city_name
    ,bd_invitation_code
    ,bd_id as bd_invitation_id
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
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--2.再取得付款方信息
retailer_info as (
  select
    id
    ,opay_id
    ,first_name as retailer_first_name
    ,last_name as retailer_last_name
    ,phone_number as retailer_phone_number
    ,retailer_email as retailer_retailer_email
    ,country as retailer_country
    ,country_name as retailer_country_name
    ,city_id as retailer_city
    ,city_name as retailer_city_name
    ,bd_id as retailer_bd_invitation_id
    ,hcm_id as retailer_hcm_id
    ,hcm_name as retailer_hcm_name
    ,cm_id as retailer_cm_id
    ,cm_name as retailer_cm_name
    ,bdm_id as retailer_bdm_id
    ,bdm_name as retailer_bdm_name
    ,bd_real_id as retailer_bd_id
    ,bd_name as retailer_bd_name
  from
    otrade_dw.dim_otrade_b2b_retailer_info_crm_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--3.零售商首单信息
retailer_first_info as (
  select
    retailer_opayid
    ,first_order_time
  from
    otrade_dw.dwm_otrade_b2b_retailer_crm_first_hf
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--4.支付信息相关
pay_info as (
  select
    order_id
    ,opay_pay_id
    ,actual_amount
    ,pay_status
    ,req_status
  from
    otrade_dw.dwd_otrade_b2b_otrade_pay_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
),

--5.订单相关
order_info as (
  select
    id
    ,pay_id
    ,order_id
    ,settle_id
    ,original_order_id
    ,order_type
    ,payer
    ,payee
    ,payer_phone
    ,payee_phone
    ,payer_name
    ,payee_name
    ,supplier_type
    ,source
    ,shop_id
    ,shop_name
    ,order_status
    ,refund_status
    ,payable_amount
    ,amount
    ,fee_type
    ,fee
    ,fee_rate
    ,handwork_fee
    ,pay_channel
    ,pay_type
    ,pay_cur
    ,pay_time
    ,consign_time
    ,confirm_time
    ,receive_time
    ,refund_type
    ,create_time
    ,update_time
  from
    otrade_dw.dwd_otrade_b2b_otrade_order_hi
  where
    concat(dt,' ',hour) >= default.minLocalTimeRange("{config}", '{v_date}', 0)
    and concat(dt,' ',hour) <= default.maxLocalTimeRange("{config}", '{v_date}', 0) 
    and utc_date_hour = date_format("{v_date}", 'yyyy-MM-dd HH')
)

--6.将数据关联后插入最终表中
insert overwrite table otrade_dw.dwd_otrade_b2b_order_collect_hi partition(country_code,dt,hour)
select
  v1.id
  ,v1.pay_id
  ,v1.order_id
  ,v1.settle_id
  ,v1.original_order_id
  ,v1.order_type
  ,v1.payer
  ,v1.payee
  ,v1.payer_phone
  ,v1.payee_phone
  ,v1.payer_name
  ,v1.payee_name
  ,v1.supplier_type
  ,v1.source
  ,v1.shop_id
  ,v1.shop_name
  ,v1.order_status
  ,v1.refund_status
  ,v1.payable_amount
  ,v1.amount
  ,v1.fee_type
  ,v1.fee
  ,v1.fee_rate
  ,v1.handwork_fee
  ,v1.pay_channel
  ,v1.pay_type
  ,v1.pay_cur
  ,v1.pay_time
  ,v1.consign_time
  ,v1.confirm_time
  ,v1.receive_time
  ,v1.refund_type
  ,v1.create_time
  ,v1.update_time

  --供应商商铺相关信息,即收款方
  ,v2.status
  ,v2.country
  ,v2.city
  ,v2.country_name
  ,v2.city_name
  ,v2.bd_invitation_code
  ,v2.bd_invitation_id
  ,v2.hcm_id
  ,v2.hcm_name
  ,v2.cm_id
  ,v2.cm_name
  ,v2.bdm_id
  ,v2.bdm_name
  ,v2.bd_id
  ,v2.bd_name

  --零售商相关信息,即付款方
  ,v3.retailer_first_name
  ,v3.retailer_last_name
  ,v3.retailer_phone_number
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

  --零售商首单相关
  ,if(v1.create_time > v4.first_order_time,0,1) as retailer_first_order
  ,v4.first_order_time as retailer_first_order_time

  --支付信息
  ,v5.opay_pay_id
  ,v5.actual_amount
  ,v5.pay_status
  ,v5.req_status

  ,date_format('{v_date}', 'yyyy-MM-dd HH') as utc_date_hour

  ,'NG' as country_code
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd') as dt
  ,date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'HH') as hour
from
  order_info as v1
left join
  supplier_info as v2
on
  v1.shop_id = v2.id
left join
  retailer_info as v3
on
  v1.payer = v3.opay_id
left join
  retailer_first_info as v4
on
  v1.payer = v4.retailer_opayid
left join
  pay_info as v5
on
  v1.order_id = v5.order_id
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwd_otrade_b2b_order_collect_hi_sql_task(ds, v_date)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


dwd_otrade_b2b_order_collect_hi_task = PythonOperator(
    task_id='dwd_otrade_b2b_order_collect_hi_task',
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

dim_otrade_b2b_supplier_info_hf_check_task >> dwd_otrade_b2b_order_collect_hi_task
dim_otrade_b2b_retailer_info_crm_hf_check_task >> dwd_otrade_b2b_order_collect_hi_task
dwm_otrade_b2b_retailer_crm_first_hf_check_task >> dwd_otrade_b2b_order_collect_hi_task
dwd_otrade_b2b_otrade_pay_hi_check_task >> dwd_otrade_b2b_order_collect_hi_task
dwd_otrade_b2b_otrade_order_hi_check_task >> dwd_otrade_b2b_order_collect_hi_task

