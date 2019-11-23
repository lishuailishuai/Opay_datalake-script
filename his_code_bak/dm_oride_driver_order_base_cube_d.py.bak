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

dag = airflow.DAG('dm_oride_driver_order_base_cube_d',
                  schedule_interval="30 01 * * *",
                  default_args=args)

##----------------------------------------- 依赖 ---------------------------------------##


# 依赖前一天分区
dependence_dwd_oride_order_base_include_test_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_base_include_test_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_order_base_include_test_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

dependence_dwd_oride_order_push_driver_detail_di_prev_day_task = UFileSensor(
    task_id='dwd_oride_order_push_driver_detail_di_prev_day_task',
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
table_name = "dm_oride_driver_order_base_cube_d"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 脚本 ---------------------------------------##

def dm_oride_driver_order_base_cube_d_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)

    SELECT nvl(product_id,-10000) AS product_id,
           nvl(driver_serv_type,-10000) as driver_serv_type,
           nvl(city_id,-10000) AS city_id,

           td_driver_take_num,
              --骑手成功应答的总次数 （push阶段）
              
           td_push_accpet_show_driver_num,
              --被推送骑手数 （accept_show阶段）

           td_driver_accept_take_num,
              --骑手应答的总次数 （accept_click阶段）

           td_request_driver_num,
              --当天接单司机数

           td_finish_order_driver_num,
              --当天完单司机数

           country_code,
           --国家码字段

           '{pt}' AS dt
    FROM
      (SELECT ord.product_id,
              ord.city_id,
              ord.driver_serv_type, -- 订单表司机业务类型
              
              count(DISTINCT p1.driver_id) AS td_driver_take_num,
              --骑手成功应答的总次数 （push阶段）
              
              count(DISTINCT r2.driver_id) AS td_push_accpet_show_driver_num,
              --被推送骑手数 （accept_show阶段）

              count(DISTINCT r1.driver_id) AS td_driver_accept_take_num,
              --骑手应答的总次数 （accept_click阶段）
              
              count(DISTINCT (if(ord.is_td_request=1,ord.driver_id,NULL))) AS td_request_driver_num,
              --当天接单司机数

              count(DISTINCT (if(ord.is_td_finish=1,ord.driver_id,NULL))) AS td_finish_order_driver_num,
              --当天完单司机数
             
              nvl(ord.country_code,-999) AS country_code --(去除with cube为空的BUG) --国家码字段

       FROM
         (
            SELECT 
            *
            FROM oride_dw.dwd_oride_order_base_include_test_di
             WHERE dt='{pt}'
             AND city_id<>'999001' --去除测试数据
             and driver_id<>1
         ) ord 
         LEFT OUTER JOIN
      (
           SELECT 
           driver_id --成功播单司机
           FROM oride_dw.dwd_oride_order_push_driver_detail_di
           WHERE dt='{pt}'
           AND success=1
           GROUP BY driver_id
       ) p1 ON ord.driver_id=p1.driver_id
       LEFT OUTER JOIN 
       (
           SELECT 
           driver_id
           FROM 
           oride_dw.dwd_oride_driver_accept_order_click_detail_di
           WHERE dt='{pt}'
           group by driver_id 
       ) r1 on ord.driver_id = r1.driver_id
       LEFT OUTER JOIN 
       (
           SELECT 
           driver_id
           FROM 
           oride_dw.dwd_oride_driver_accept_order_show_detail_di
           WHERE dt='{pt}'
           group by driver_id 
       ) r2 on ord.driver_id = r2.driver_id
       GROUP BY ord.product_id,
                ord.city_id,
                ord.driver_serv_type,
                ord.country_code 
                WITH CUBE) x
    WHERE x.country_code IN ('nal')
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
    _sql = dm_oride_driver_order_base_cube_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dm_oride_driver_order_base_cube_d_task = PythonOperator(
    task_id='dm_oride_driver_order_base_cube_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dwd_oride_order_base_include_test_di_prev_day_task >> \
dependence_dwd_oride_order_push_driver_detail_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_show_detail_di_prev_day_task >> \
dependence_dwd_oride_driver_accept_order_click_detail_di_prev_day_task >> \
dm_oride_driver_order_base_cube_d_task