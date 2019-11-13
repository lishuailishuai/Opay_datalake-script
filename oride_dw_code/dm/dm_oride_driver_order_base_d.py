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
    'start_date': datetime(2019, 10, 17),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dm_oride_driver_order_base_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_di_prev_day_tesk = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前一天分区
dependence_oride_driver_timerange_prev_day_tesk = HivePartitionSensor(
    task_id="oride_driver_timerange_prev_day_tesk",
    table="ods_log_oride_driver_timerange",
    partition="dt='{{ds}}'",
    schema="oride_dw_ods",
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_order_push_driver_detail_di_prev_day_tesk = UFileSensor(
    task_id='dwd_oride_order_push_driver_detail_di_prev_day_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_push_driver_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_show_detail_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_driver_accept_order_click_detail_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_accept_order_click_detail_di/country_code=nal",
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
table_name = "dm_oride_driver_order_base_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def dm_oride_driver_order_base_d_sql_task(ds):
    HQL ='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

    SELECT ord.product_id, --订单表下单业务类型
            ord.driver_serv_type, --订单表司机业务类型 
            ord.city_id, --城市ID
            
            sum(nvl(ord.td_finish_order_dur,0)) AS driver_finish_order_dur,
            --完单做单时长(秒）

            sum(nvl(ord.td_cannel_pick_dur,0)) AS driver_cannel_pick_dur,
            --取消订单时长（秒）

            sum(nvl(dtr.driver_freerange,0)) AS driver_free_dur, --司机总在线时长=完单做单时长+取消订单时长+司机空闲时长
            --司机空闲时长（秒）
            
            sum(if(ord.is_td_finish>=1,nvl(dtr.driver_freerange,0) + nvl(ord.td_service_dur,0) + nvl(ord.td_cannel_pick_dur,0),0)) AS finish_driver_online_dur,
            --完单司机在线时长（秒）

            sum(if(p1.driver_id is not null,ord.succ_push_order_cnt,0)) AS succ_push_order_cnt,--成功推送司机的订单数
         
            sum(nvl(s1.driver_pushed_order_cnt,0)) as driver_pushed_order_cnt,
            --司机被推送订单总数（accpet_show阶段，算法要求此指标为订单总推送）

            sum(nvl(c1.driver_click_order_cnt,0)) as driver_click_order_cnt,
            --司机点击接受订单总数（accpet_click阶段，算法要求此指标为订单总应答）

            sum(nvl(ord.td_billing_dur,0)) as driver_billing_dur,
            --司机订单计费时长
            
            sum(if(ord.is_td_finish>=1 and ord.is_strong_dispatch>=1,nvl(dtr.driver_freerange,0) + nvl(ord.td_service_dur,0) + nvl(ord.td_cannel_pick_dur,0),0)) AS strong_finish_driver_online_dur,
            --强派单完单司机在线时长（秒）
            ord.country_code as country_code, --国家编码
            '{pt}' as dt

       FROM
            (
                SELECT product_id,  --订单表下单业务类型
                driver_serv_type, --订单表司机业务类型
                city_id,  --城市
                driver_id,  --司机id
                country_code,  --国家编码
                count(order_id) as succ_push_order_cnt, --订单量
                sum(if(is_td_finish = 1,td_finish_order_dur,0)) as td_finish_order_dur, --完单在线时长
                sum(td_billing_dur) as td_billing_dur, --计费时长
                sum(td_cannel_pick_dur) as td_cannel_pick_dur, --当天取消接驾时长
                sum(is_strong_dispatch) as is_strong_dispatch,  --用于判断该司机是否是强派单司机
                sum(is_td_finish) as is_td_finish,  --用于判断该订单是否是完单
                sum(td_service_dur) as td_service_dur --司机服务时长

                FROM oride_dw.dwd_oride_order_base_include_test_di
                WHERE dt='{pt}'
                AND city_id<>'999001' --去除测试数据
                and driver_id<>1 
                group by product_id,
                driver_serv_type,
                city_id,
                driver_id,
                country_code
            ) ord 
            LEFT OUTER JOIN
            (
                SELECT *
                FROM oride_dw_ods.ods_log_oride_driver_timerange
                WHERE dt='{pt}'
            ) dtr ON ord.driver_id=dtr.driver_id
            LEFT OUTER JOIN
            (
                SELECT driver_id --成功播单司机
                FROM oride_dw.dwd_oride_order_push_driver_detail_di
                WHERE dt='{pt}'
                AND success=1
                GROUP BY driver_id
            ) p1 ON ord.driver_id=p1.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct(order_id)) driver_pushed_order_cnt
                FROM 
                oride_dw.dwd_oride_driver_accept_order_show_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) s1 on ord.driver_id=s1.driver_id
            LEFT OUTER JOIN
            (
                SELECT 
                driver_id,
                count(distinct(order_id)) driver_click_order_cnt
                FROM 
                oride_dw.dwd_oride_driver_accept_order_click_detail_di
                WHERE dt='{pt}'
                GROUP BY driver_id
            ) c1 on ord.driver_id=c1.driver_id

       GROUP BY ord.product_id,
                ord.driver_serv_type,
                ord.city_id,
                ord.country_code;
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
    _sql = dm_oride_driver_order_base_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dm_oride_driver_order_base_d_task = PythonOperator(
    task_id='dm_oride_driver_order_base_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_order_base_include_test_di_prev_day_tesk >> \
dependence_oride_driver_timerange_prev_day_tesk >> \
dependence_dwd_oride_order_push_driver_detail_di_prev_day_tesk >> \
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task >> \
dm_oride_driver_order_base_d_task
