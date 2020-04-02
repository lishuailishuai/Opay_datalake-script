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
    'start_date': datetime(2020, 3, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_otrade_b2b_order_target_supplier_bd_di',
                  schedule_interval="00 03 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##
db_name = "otrade_dw"
table_name = "app_otrade_b2b_order_target_supplier_bd_di"
hdfs_path = "oss://opay-datalake/otrade/otrade_dw/" + table_name
config = eval(Variable.get("otrade_time_zone_config"))
time_zone = config['NG']['time_zone']

##----------------------------------------- 依赖 ---------------------------------------##

dim_otrade_b2b_supplier_info_hf_task = OssSensor(
    task_id='dim_otrade_b2b_supplier_info_hf_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dim_otrade_b2b_supplier_info_hf",
        pt='{{ds}}',
        hour='23'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dwd_otrade_b2b_order_collect_di_task = OssSensor(
    task_id='dwd_otrade_b2b_order_collect_di_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="otrade/otrade_dw/dwd_otrade_b2b_order_collect_di",
        pt='{{ds}}'
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

def app_otrade_b2b_order_target_supplier_bd_di_sql_task(ds):
    HQL = '''

set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.strict.checks.cartesian.product=false;

--0.查看店铺信息
with 
shop_info as (
  select
    hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_id
    ,bd_name
  
    ,country
    ,country_name
    ,city
    ,city_name

    ,country_code

    ,count(if(substr(create_time,0,10) = '{pt}',1,null)) as new_shop_cnt
    ,count(1) as total_shop_cnt
  from
    otrade_dw.dim_otrade_b2b_supplier_info_hf
  where
    dt = '{pt}'
    and hour = '23'
  group by
    hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_id
    ,bd_name
  
    ,country
    ,country_name
    ,city
    ,city_name

    ,country_code

),

--1.销售情况分析
order_info as (
  select
    hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_id
    ,bd_name
  
    ,country
    ,country_name
    ,city
    ,city_name
  
    --下单分析
    ,sum(if(order_type='pay',payable_amount,0)) as order_amt
    ,count(if(order_type='pay',1,null)) as order_cnt
    ,count(distinct(if(order_type='pay',payer,null))) as order_people
    ,sum(if(order_type='pay' and retailer_first_order=1,payable_amount,0)) as first_order_amt
    ,count(if(order_type='pay' and retailer_first_order=1,1,null)) as first_order_cnt
    ,count(distinct(if(order_type='pay' and retailer_first_order=1,payer,null))) as first_order_people
  
    --销售分析
    ,sum(if(order_type='pay',amount,0)) as pay_amt
    ,count(if(order_type='pay' and pay_status = 3,1,null)) as pay_suc_cnt
    ,count(if(order_type='pay' and pay_time is not null,1,null)) as pay_cnt
  
    --退款分析
    ,count(if(order_type='pay' and order_status=0,1,null)) as refund_order_cnt
    ,count(if(order_type='refund' and order_status=2,1,null)) as refund_suc_cnt
    ,sum(if(order_type='refund' and order_status=2,payable_amount,0)) as refund_order_amt
  
    --收货分析
    ,count(if(order_type='pay' and consign_time='{pt}',1,null)) as delivery_order_cnt
    ,count(if(order_type='pay' and confirm_time='{pt}',1,null)) as receive_order_cnt
    ,sum(if(order_type='pay' and confirm_time='{pt}',payable_amount,0)) as receive_order_amt
  
    --店铺分析
    ,0 as new_shop_cnt
    ,count(distinct(if(order_type='pay',shop_id,null))) as order_shop_cnt
    ,count(distinct(if(order_type='pay' and first_order = 1,shop_id,null))) as first_order_shop_cnt
    ,0 as total_shop_cnt
  
    --用户分析
    ,count(distinct(if(order_type='pay' and pay_time is not null,payer,null))) as pay_people
    ,count(distinct(if(order_type='pay' and pay_status = 3,payer,null))) as pay_suc_people
  
    ,country_code
    ,dt
  from
    otrade_dw.dwd_otrade_b2b_order_collect_di
  where
    dt = '{pt}'
  group by
    hcm_id
    ,hcm_name
    ,cm_id
    ,cm_name
    ,bdm_id
    ,bdm_name
    ,bd_id
    ,bd_name
    ,country
    ,country_name
    ,city
    ,city_name
    
    ,country_code
    ,dt
)

--2.插入数据
insert overwrite table otrade_dw.app_otrade_b2b_order_target_supplier_bd_di partition(country_code,dt)
select
  nvl(v1.hcm_id,v2.hcm_id) as hcm_id
  ,nvl(v1.hcm_name,v2.hcm_name) as hcm_name
  ,nvl(v1.cm_id,v2.cm_id) as cm_id
  ,nvl(v1.cm_name,v2.cm_name) as cm_name
  ,nvl(v1.bdm_id,v2.bdm_id) as bdm_id
  ,nvl(v1.bdm_name,v2.bdm_name) as bdm_name
  ,nvl(v1.bd_id,v2.bd_id) as bd_id
  ,nvl(v1.bd_name,v2.bd_name) as bd_name
  ,nvl(v1.country,v2.country) as country
  ,nvl(v1.country_name,v2.country_name) as country_name
  ,nvl(v1.city,v2.city) as city
  ,nvl(v1.city_name,v2.city_name) as city_name

  ,nvl(v1.order_amt,0) as order_amt
  ,nvl(v1.order_cnt,0) as order_cnt
  ,nvl(v1.order_people,0) as order_people
  ,nvl(v1.first_order_amt,0) as first_order_amt
  ,nvl(v1.first_order_cnt,0) as first_order_cnt
  ,nvl(v1.first_order_people,0) as first_order_people
  ,nvl(v1.pay_amt,0) as pay_amt
  ,nvl(v1.pay_suc_cnt,0) as pay_suc_cnt
  ,nvl(v1.pay_cnt,0) as pay_cnt
  ,nvl(v1.refund_order_cnt,0) as refund_order_cnt
  ,nvl(v1.refund_suc_cnt,0) as refund_suc_cnt
  ,nvl(v1.refund_order_amt,0) as refund_order_amt
  ,nvl(v1.delivery_order_cnt,0) as delivery_order_cnt
  ,nvl(v1.receive_order_cnt,0) as receive_order_cnt
  ,nvl(v1.receive_order_amt,0) as receive_order_amt
  ,nvl(v2.new_shop_cnt,0) as new_shop_cnt
  ,nvl(v1.order_shop_cnt,0) as order_shop_cnt
  ,nvl(v1.first_order_shop_cnt,0) as first_order_shop_cnt
  ,nvl(v2.total_shop_cnt,0) as total_shop_cnt
  ,nvl(v1.pay_people,0) as pay_people
  ,nvl(v1.pay_suc_people,0) as pay_suc_people
  
  ,'NG' as country_code
  ,'{pt}' as dt
from
  order_info as v1
full join
  shop_info as v2
on
  v1.hcm_id = v2.hcm_id
  and v1.cm_id = v2.cm_id
  and v1.bdm_id = v2.bdm_id
  and v1.bd_id = v2.bd_id
  and v1.city = v2.city
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
    # _sql = app_otrade_b2b_order_target_supplier_bd_di_sql_task(ds)

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
    _sql = "\n" + cf.alter_partition() + "\n" + app_otrade_b2b_order_target_supplier_bd_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()


app_otrade_b2b_order_target_supplier_bd_di_task = PythonOperator(
    task_id='app_otrade_b2b_order_target_supplier_bd_di_task',
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

dim_otrade_b2b_supplier_info_hf_task >> app_otrade_b2b_order_target_supplier_bd_di_task
dwd_otrade_b2b_order_collect_di_task >> app_otrade_b2b_order_target_supplier_bd_di_task





