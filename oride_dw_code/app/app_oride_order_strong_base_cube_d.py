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
    'start_date': datetime(2019, 9, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_order_strong_base_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dm_oride_order_strong_base_cube_d_prev_day_task = UFileSensor(
    task_id='dm_oride_order_strong_base_cube_d_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_order_strong_base_cube_d/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)
# 依赖前一天分区
dependence_dm_oride_driver_base_prev_day_task = UFileSensor(
    task_id='dm_oride_driver_base_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dm_oride_driver_base/country_code=nal",
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
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3000"}
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
table_name = "app_oride_order_strong_base_cube_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_order_strong_base_cube_d_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
          SELECT nvl(a.city_id,-10000) as city_id,
                 nvl(a.product_id,-10000) as product_id,
                 sum(a.strong_dispatch_driver_cnt) as strong_dispatch_driver_cnt, --强派单司机数
                 sum(a.strong_dispatch_order_cnt) as strong_dispatch_order_cnt, --强派单调度推单数
                 sum(a.finished_driver_cnt) as finished_driver_cnt, --完单司机数 
                 sum(a.strong_finished_driver_cnt) as strong_finished_driver_cnt, --强派单完单司机数
                 sum(a.strong_finished_order_cnt) as strong_finished_order_cnt, --强派单完单数
                 sum(a.strong_finish_order_pick_up_dis) as strong_finish_order_pick_up_dis, --强派单完单接驾距离(米)
                 sum(a.strong_finish_order_pick_up_assigned_cnt) as strong_finish_order_pick_up_assigned_cnt, --强派单订单被分配次数（计算平均接驾距离使用）
                 sum(a.strong_user_cancel_order_cnt) as strong_user_cancel_order_cnt, --强派单乘客取消订单数
                 sum(a.strong_driver_cancel_order_cnt) as strong_driver_cancel_order_cnt, --强派单司机取消订单数
                 sum(a.strong_paid_order_cnt) as strong_paid_order_cnt, --强派单支付订单数
                 sum(a.strong_paid_price) as strong_paid_price,  --强派单应付金额
                 sum(a.strong_paid_amount) as strong_paid_amount,  --强派单实付金额
                 sum(b.finish_driver_online_dur) as finish_driver_online_dur, --当日完单司机在线时长
                 sum(b.strong_finish_driver_online_dur) as strong_finish_driver_online_dur, --当日强制派单完单司机在线时长
                 sum(a.push_show_ord_cnt) as push_show_ord_cnt, --push到达单数（派单）
                 sum(a.accept_show_ord_cnt) as accept_show_ord_cnt, --展示单数（派单）
                 sum(a.show_ord_cnt) as show_ord_cnt, --推送订单数（派单）
                 sum(a.accept_click_ord_cnt) as accept_click_ord_cnt, --接单数（派单）
                 'nal' as country_code,
                 '{pt}' as dt
        FROM
          (SELECT *
           FROM oride_dw.dm_oride_order_strong_base_cube_d
           WHERE dt='{pt}') a
        LEFT JOIN
          (SELECT nvl(country_code,'-10000') AS country_code,
                  nvl(cast(city_id AS bigint),-10000) AS city_id,
                  nvl(product_id,-10000) AS product_id,
                  sum(finish_driver_online_dur) AS finish_driver_online_dur, --当日完单司机在线时长
         sum(strong_finish_driver_online_dur) AS strong_finish_driver_online_dur --当日强制派单完单司机在线时长
        FROM oride_dw.dm_oride_driver_base
           WHERE dt='{pt}'
           GROUP BY nvl(country_code,'-10000'),
                    nvl(cast(city_id AS bigint),-10000),
                    nvl(product_id,-10000) WITH CUBE) b ON a.city_id=nvl(b.city_id,-10000)
        AND a.product_id=nvl(b.product_id,-10000)
        AND a.country_code=nvl(b.country_code,-10000)
        group by nvl(a.city_id,-10000),nvl(a.product_id,-10000)
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
    _sql = app_oride_order_strong_base_cube_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_oride_order_strong_base_cube_d_task = PythonOperator(
    task_id='app_oride_order_strong_base_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dm_oride_order_strong_base_cube_d_prev_day_task >> \
dependence_dm_oride_driver_base_prev_day_task >> \
app_oride_order_strong_base_cube_d_task

