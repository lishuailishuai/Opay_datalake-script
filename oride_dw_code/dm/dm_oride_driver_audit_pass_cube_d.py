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
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 9, 9),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_driver_audit_pass_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwm_oride_driver_base_df_prev_day_task = UFileSensor(
    task_id='dwm_oride_driver_base_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dm_oride_driver_audit_pass_cube_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def dm_oride_driver_audit_pass_cube_d_sql_task(ds):
    HQL ='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

    select nvl(product_id,-10000) as product_id,
       nvl(city_id,-10000) as city_id,
       audit_finish_driver_num,
       --注册司机数，审核通过司机数，但是注册司机数理论用该表统计不太准确
       
       bind_driver_num,
       --绑定成功司机数
       
       n_bind_driver_num,
       --未绑定司机数
       
       td_online_driver_num,
       --当天在线司机数
       
       td_driver_accept_take_num,
       --骑手端应答的司机数 （accept_click阶段）
       --之前统计有问题，偏小，不应该用订单表中司机ID关联
       
       td_driver_take_num,
       --成功被播单司机数 （push阶段）
       --之前统计有问题，不应该用订单表关联push节点司机，订单表中有司机的都是接单的，因此统计偏少
       
       td_request_driver_num,
       --当天接单司机数
       
       td_finish_order_driver_num,
       --当天完单司机数
       
       td_push_accpet_show_driver_num,
       --当天被推送骑手数（骑手端show打点）
       --之前统计有问题，偏小，不应该用订单表中司机ID关联
       
       td_audit_finish_driver_num,
       --当天注册司机数,当天审核通过司机数
       
       fraud_driver_cnt, 
       --疑似作弊订单涉及司机数

       nvl(country_code,-10000) AS country_code,
       --(去除with cube为空的BUG) --国家码字段
       
       dt

        from (select product_id,
               city_id,
               count(driver_id) as audit_finish_driver_num,
               --注册司机数，审核通过司机数，但是注册司机数理论用该表统计不太准确
               
               sum(is_bind) as bind_driver_num,
               --绑定成功司机数
               
               sum(if(is_bind=1,0,1)) AS n_bind_driver_num,
               --未绑定司机数
               
               sum(is_td_online) as td_online_driver_num,
               --当天在线司机数
               
               sum(is_td_accpet_click) as td_driver_accept_take_num,
               --骑手端应答的司机数 （accept_click阶段）
               --之前统计有问题，偏小，不应该用订单表中司机ID关联
               
               sum(is_td_succ_broadcast) as td_driver_take_num,
               --成功被播单司机数 （push阶段）
               --之前统计有问题，不应该用订单表关联push节点司机，订单表中有司机的都是接单的，因此统计偏少
               
               sum(is_td_request) as td_request_driver_num,
               --当天接单司机数
               
               sum(is_td_finish) as td_finish_order_driver_num,
               --当天完单司机数
               
               sum(is_td_accpet_show) as td_push_accpet_show_driver_num,
               --当天被推送骑手数（骑手端show打点）
               --之前统计有问题，偏小，不应该用订单表中司机ID关联
               
               sum(is_td_sign) as td_audit_finish_driver_num,
               --当天注册司机数,当天审核通过司机数
               
               null as fraud_driver_cnt, 
               --疑似作弊订单涉及司机数
        
               nvl(country_code,-999) AS country_code,
               --(去除with cube为空的BUG) --国家码字段
        
               '{pt}' AS dt
               
        from oride_dw.dwm_oride_driver_base_df
        where dt='{pt}'
        group by product_id,
               city_id,
               country_code
        with cube) x
        WHERE x.country_code IN ('nal');
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )
    return HQL

# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dm_oride_driver_audit_pass_cube_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dm_oride_driver_audit_pass_cube_d_task = PythonOperator(
    task_id='dm_oride_driver_audit_pass_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dim_oride_driver_base_prev_day_task >> \
dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dwd_oride_order_push_driver_detail_di_prev_day_task >> \
dependence_ods_log_oride_driver_timerange_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task >> \
dm_oride_driver_audit_pass_cube_d_task
