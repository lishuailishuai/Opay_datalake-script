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
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 11, 14),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_driver_base',
                  schedule_interval="50 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwm_oride_driver_base_df_prev_day_task = UFileSensor(
    task_id='dwm_oride_driver_base_df_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dim_oride_city_prev_day_tesk = UFileSensor(
    task_id='dim_oride_city_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dim_oride_city/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dm_oride_driver_base"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def dm_oride_driver_base_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

    select dri.product_id,
       dri.city_id,
       count(driver_id) as audit_finish_driver_num,
       --注册司机数，审核通过司机数
       
       sum(is_td_sign) as td_audit_finish_driver_num,
       --当天注册司机数,当天审核通过司机数
       
       sum(is_bind) as bind_driver_num,
       --绑定成功司机数
       
       sum(if(is_bind=1,0,1)) AS n_bind_driver_num,
       --未绑定司机数
       
       sum(is_td_online) as td_online_driver_num,
       --当天在线司机数
       
       sum(is_td_request) as td_request_driver_num,
       --当天接单司机数
       
       sum(is_td_finish) as td_finish_driver_num,
       --当天完单司机数
       
       sum(is_td_succ_broadcast) as td_succ_broadcast_driver_num,
       --成功被播单司机数 （push阶段）
       --之前统计有问题，不应该用订单表关联push节点司机，订单表中有司机的都是接单的，因此统计偏少
    
       sum(is_td_accpet_show) as td_accpet_show_driver_num,
       --当天被推送骑手数（骑手端show打点）
       --之前统计有问题，偏小，不应该用订单表中司机ID关联
       
       sum(is_td_accpet_click) as td_accpet_click_driver_num,
       --骑手端应答的司机数 （accept_click阶段）
       --之前统计有问题，偏小，不应该用订单表中司机ID关联
                          
       sum(driver_finished_dur) as driver_finished_dur,
       --司机支付完单做单时长
       
       sum(driver_cannel_pick_dur) as driver_cannel_pick_dur,
       --司机订单取消接驾时长，包含各种取消方数据
       
       sum(driver_free_dur) as driver_free_dur,
       --司机空闲时长
       
       sum(driver_service_dur) as driver_service_dur,
       --司机服务时长
       
       sum(driver_billing_dur) as driver_billing_dur,
       --司机计费时长
       
       sum(succ_push_order_cnt) as driver_pushed_order_cnt,  
       --司机被推送订单量(push节点)  字段名称修改 
       
       sum(driver_show_order_cnt) as driver_showed_order_cnt,  
       --司机被推送订单量(accept_show节点)  字段名称修改 
       
       sum(driver_click_order_cnt) as driver_click_order_cnt,
       --司机点击应答订单量(accept_click节点)
       
       sum(driver_request_order_cnt) as driver_request_order_cnt,  
       --司机接单量  
       
       sum(nvl(driver_service_dur,0)+nvl(driver_cannel_pick_dur,0)+nvl(driver_free_dur,0)) as driver_online_dur,
       --司机在线时长
       
       sum(driver_finish_online_dur) as finish_driver_online_dur,
       --完单司机在线时长
  
       sum(strong_driver_finish_online_dur) as strong_finish_driver_online_dur,
       --强派单完单司机在线时长
       
       country_code AS country_code,
       --(去除with cube为空的BUG) --国家码字段
       
       '{pt}' AS dt
       from (select *         
       from oride_dw.dwm_oride_driver_base_df
       where dt='{pt}') dri
       inner join
       (SELECT product_id1,
               city_id
        FROM oride_dw.dim_oride_city 
        LATERAL VIEW explode(split(regexp_replace(product_id,'\\\\[|\\\\]',''),',')) s AS product_id1
        WHERE dt='{pt}' and city_id<>999001) cit
        on dri.product_id=cit.product_id1 and dri.city_id=cit.city_id
       group by dri.product_id,
               dri.city_id,
               country_code

'''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kwargs):
    v_date = kwargs.get('v_execution_date')
    v_day = kwargs.get('v_execution_day')
    v_hour = kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()
    """
            #功能函数
            alter语句: alter_partition
            删除分区: delete_partition
            生产success: touchz_success

            #参数
            第一个参数true: 所有国家是否上线。false 没有
            第二个参数true: 数据目录是有country_code分区。false 没有
            第三个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

            #读取sql
            %_sql(ds,v_hour)

            第一个参数ds: 天级任务
            第二个参数v_hour: 小时级任务，需要使用

        """
    cf = CountriesPublicFrame("true", ds, db_name, table_name, hdfs_path, "true", "true")

    # 删除分区
    cf.delete_partition()

    # 读取sql
    _sql = "\n" + cf.alter_partition() + "\n" + dm_oride_driver_base_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

dm_oride_driver_base_task = PythonOperator(
    task_id='dm_oride_driver_base_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dependence_dwm_oride_driver_base_df_prev_day_task >> dim_oride_city_prev_day_tesk >> dm_oride_driver_base_task