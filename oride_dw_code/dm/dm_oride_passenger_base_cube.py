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
from airflow.sensors import OssSensor
from airflow.sensors.s3_key_sensor import S3KeySensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesPublicFrame import CountriesPublicFrame
from plugins.CountriesAppFrame import CountriesAppFrame

import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 12, 4),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_passenger_base_cube',
                  schedule_interval="45 00 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dm_oride_passenger_base_cube"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖前一天分区
    dwm_oride_passenger_order_base_di_prev_day_task = UFileSensor(
        task_id='dwm_oride_passenger_order_base_di_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_passenger_order_base_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    #路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    # 依赖前一天分区
    dwm_oride_passenger_order_base_di_prev_day_task = OssSensor(
        task_id='dwm_oride_passenger_order_base_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_passenger_order_base_di/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dm_oride_passenger_base_cube_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    SET hive.map.aggr=true;
    --设置map端输出进行合并，默认为true  
    set hive.merge.mapfiles = true;  
    --设置reduce端输出进行合并，默认为false  
    set hive.merge.mapredfiles = true ;
    set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    
    with passenger_data as
    (
    select city_id,
       product_id,
      -- driver_serv_type,  --如果要看下单情况必须用product_id来看，如果看完单情况需要看招手停就通过product_id看，否则用driver_serv_type看
       count(distinct (if(order_cnt>0,passenger_id,null))) as ord_users, --当日下单乘客数
       count(distinct (if(finish_order_cnt>0,passenger_id,null))) as finished_users, --当日完单乘客数
       count(distinct (if(is_first_finish_user=1,passenger_id,null))) as first_finished_users, --当日订单中首次完单乘客数
       sum(new_user_ord_cnt) as new_user_ord_cnt, --当日新注册乘客下单量
       sum(new_user_finished_cnt) as new_user_finished_cnt, --当日新注册乘客完单量
       sum(new_user_gmv) as new_user_gmv, --当日注册乘客完单gmv，包含状态4，5
       count(distinct (if(pay_succ_ord_cnt>0,passenger_id,null))) as paid_users, --当日所有支付成功乘客数
       count(distinct (if(opay_pay_succ_ord_cnt>0,passenger_id,null))) as opay_paid_users, --当日opay支付成功乘客数，自1226号开始改变名称，逻辑不变
       count(distinct (if(online_pay_succ_ord_cnt>0,passenger_id,null))) as online_paid_users, --当日线上支付成功乘客数，自12.26号开始该表接入
       count(distinct (if(nobeckon_pay_succ_ord_cnt>0,passenger_id,null))) as nobeckon_paid_users, --当日所有支付成功乘客数，自12.26号开始该表接入
       count(distinct (if(nobeckon_opay_pay_succ_ord_cnt>0,passenger_id,null))) as nobeckon_opay_paid_users, --当日opay支付成功乘客数，自1226号开始改变名称，逻辑不变
       count(distinct (if(nobeckon_online_pay_succ_ord_cnt>0,passenger_id,null))) as nobeckon_online_paid_users, --当日线上支付成功乘客数，自12.26号开始该表接入     
       sum(new_user_online_pay_price) as new_user_online_pay_price, --当日注册乘客线上支付成功gmv，自12.26号开始该表接入
       sum(falsify) as falsify, --用户罚款，自12.26号开始该表接入
       sum(falsify_driver_cancel) as falsify_driver_cancel, --司机罚款，自12.26号开始该表接入
       --if(dt<'2019-12-01' and country_code='nalNG',country_code) as country_code    
       country_code   

from oride_dw.dwm_oride_passenger_order_base_di
        where dt='{pt}'
        group by city_id,
               product_id,
              -- driver_serv_type,
               --if(dt<'2019-12-01' and country_code='nal','NG',country_code)
               country_code
        with cube
    )
    
    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
    select nvl(city_id,-10000) as city_id,
           nvl(product_id,-10000) as product_id,
           -10000 as driver_serv_type,
           ord_users, --当日下单乘客数
           finished_users, --当日完单乘客数
           first_finished_users, --当日订单中首次完单乘客数
           new_user_ord_cnt, --当日新注册乘客下单量
           new_user_finished_cnt, --当日新注册乘客完单量
           new_user_gmv, --当日注册乘客完单gmv
           paid_users, --当日所有支付成功乘客数
           opay_paid_users, --当日opay支付成功乘客数，改名称，自1226号开始改变名称，逻辑不变
           online_paid_users, --当日线上支付成功乘客数，自12.26号开始该表接入
           nobeckon_paid_users, --当日所有支付成功乘客数，自12.26号开始该表接入
           nobeckon_opay_paid_users, --当日opay支付成功乘客数，自1226号开始改变名称，逻辑不变
           nobeckon_online_paid_users, --当日线上支付成功乘客数，自12.26号开始该表接入 
           new_user_online_pay_price, --当日注册乘客线上支付成功gmv，自12.26号开始该表接入
           falsify, --用户罚款，自12.26号开始该表接入
           falsify_driver_cancel, --司机罚款，自12.26号开始该表接入
           nvl(country_code,'total') as country_code,
           '{pt}' as dt
    from passenger_data t;
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 熔断数据，如果数据为0，报错
def check_key_data_cnt_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] == 0:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag

#主流程
def execution_data_task_id(ds,dag,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

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
            "business_key": "oride"
        }
    ]

    cf = CountriesAppFrame(args)

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dm_oride_passenger_base_cube_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

dm_oride_passenger_base_cube_task = PythonOperator(
    task_id='dm_oride_passenger_base_cube_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_passenger_order_base_di_prev_day_task >> dm_oride_passenger_base_cube_task


