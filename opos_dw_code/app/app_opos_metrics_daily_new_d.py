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
    'owner': 'yuanfeng',
    'start_date': datetime(2019, 11, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_opos_metrics_daily_new_d',
                  schedule_interval="20 04 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

opos_metrcis_report_task = UFileSensor(
    task_id='opos_metrcis_report_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_temp/opos_metrcis_report",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

opos_active_user_daily_task = UFileSensor(
    task_id='opos_active_user_daily_task',
    filepath='{hdfs_path_str}/country_code=nal/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="opos/opos_temp/opos_active_user_daily",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "opos_dw"
table_name = "app_opos_metrics_daily_new_d"
hdfs_path = "ufile://opay-datalake/opos/opos_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"db": "opos_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_opos_metrics_daily_new_d_sql_task(ds):
    HQL = '''
set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;


insert overwrite table opos_dw.app_opos_metrics_daily_new_d partition(country_code,dt)
select 
0 as id
,d.week_of_year as create_week
,substr(a.dt,0,7) as create_month
,substr(a.dt,0,4) as create_year

,nvl(a.hcm_id,b.hcm_id) as hcm_id
,nvl(a.hcm_name,b.hcm_name) as hcm_name
,nvl(a.cm_id,b.cm_id) as cm_id
,nvl(a.cm_name,b.cm_name) as cm_name
,nvl(a.rm_id,b.rm_id) as rm_id
,nvl(a.rm_name,b.rm_name) as rm_name
,nvl(a.bdm_id,b.bdm_id) as bdm_id
,nvl(a.bdm_name,b.bdm_name) as bdm_name
,nvl(a.bd_id,b.bd_id) as bd_id
,nvl(a.bd_name,b.bd_name) as bd_name

,nvl(a.city_id,b.city_id) as city_id
,nvl(a.city_name,b.city_name) as city_name
,nvl(a.country,b.country) as country

,nvl(a.merchant_cnt,0) as merchant_cnt
,nvl(a.pos_merchant_cnt,0) as pos_merchant_cnt
,nvl(a.new_merchant_cnt,0) as new_merchant_cnt
,nvl(a.new_pos_merchant_cnt,0) as new_pos_merchant_cnt
,nvl(a.pos_complete_order_cnt,0) as pos_complete_order_cnt
,nvl(a.qr_complete_order_cnt,0) as qr_complete_order_cnt
,nvl(a.complete_order_cnt,0) as complete_order_cnt
,nvl(a.gmv,0) as gmv
,nvl(a.actual_amount,0) as actual_amount

,nvl(b.pos_user_active_cnt,0) as pos_user_active_cnt
,nvl(b.qr_user_active_cnt,0) as qr_user_active_cnt
,nvl(b.before_1_day_user_active_cnt,0) as before_1_day_user_active_cnt
,nvl(b.before_7_day_user_active_cnt,0) as before_7_day_user_active_cnt
,nvl(b.before_15_day_user_active_cnt,0) as before_15_day_user_active_cnt
,nvl(b.before_30_day_user_active_cnt,0) as before_30_day_user_active_cnt
,nvl(b.order_merchant_cnt,0) as order_merchant_cnt
,nvl(b.pos_order_merchant_cnt,0) as pos_order_merchant_cnt
,nvl(b.week_pos_user_active_cnt,0) as week_pos_user_active_cnt
,nvl(b.week_qr_user_active_cnt,0) as week_qr_user_active_cnt
,nvl(b.month_pos_user_active_cnt,0) as month_pos_user_active_cnt
,nvl(b.month_qr_user_active_cnt,0) as month_qr_user_active_cnt
,nvl(b.have_order_user_cnt,0) as have_order_user_cnt

,nvl(a.return_amount,0) as return_amount
,nvl(a.new_user_cost,0) as new_user_cost
,nvl(a.old_user_cost,0) as old_user_cost
,nvl(a.return_amount_order_cnt,0) as return_amount_order_cnt

,nvl(a.his_pos_complete_order_cnt,0) as his_pos_complete_order_cnt
,nvl(a.his_qr_complete_order_cnt,0) as his_qr_complete_order_cnt
,nvl(a.his_complete_order_cnt,0) as his_complete_order_cnt
,nvl(a.his_gmv,0) as his_gmv
,nvl(a.his_actual_amount,0) as his_actual_amount
,nvl(a.his_return_amount,0) as his_return_amount
,nvl(a.his_new_user_cost,0) as his_new_user_cost
,nvl(a.his_old_user_cost,0) as his_old_user_cost
,nvl(a.his_return_amount_order_cnt,0) as his_return_amount_order_cnt

,nvl(b.user_active_cnt,0) as user_active_cnt
,nvl(b.new_user_cnt,0) as new_user_cnt
,nvl(b.more_5_merchant_cnt,0) as more_5_merchant_cnt
,nvl(b.his_order_merchant_cnt,0) as his_order_merchant_cnt
,nvl(b.his_pos_order_merchant_cnt,0) as his_pos_order_merchant_cnt
,nvl(b.his_user_active_cnt,0) as his_user_active_cnt
,nvl(b.his_pos_user_active_cnt,0) as his_pos_user_active_cnt
,nvl(b.his_qr_user_active_cnt,0) as his_qr_user_active_cnt

,'nal' as country_code
,'{pt}' as dt
from 
(select * from opos_temp.opos_metrcis_report where country_code = 'nal' and  dt = '{pt}') a
full join 
(select * from opos_temp.opos_active_user_daily where country_code = 'nal' and  dt = '{pt}') b 
on  
a.hcm_id=b.hcm_id
AND a.cm_id=b.cm_id
AND a.rm_id=b.rm_id
AND a.bdm_id=b.bdm_id
AND a.bd_id=b.bd_id
and a.city_id=b.city_id
left join
(select dt,week_of_year from public_dw_dim.dim_date where dt = '{pt}') as d
on
1=1;



'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
        db=db_name
    )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = app_opos_metrics_daily_new_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    # check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


app_opos_metrics_daily_new_d_task = PythonOperator(
    task_id='app_opos_metrics_daily_new_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

opos_metrcis_report_task >> app_opos_metrics_daily_new_d_task
opos_active_user_daily_task >> app_opos_metrics_daily_new_d_task

# 查看任务命令
# airflow list_tasks app_opos_metrics_daily_new_d -sd /home/feng.yuan/app_opos_metrics_daily_new_d.py
# 测试任务命令
# airflow test app_opos_metrics_daily_new_d app_opos_metrics_daily_new_d_task 2019-11-24 -sd /home/feng.yuan/app_opos_metrics_daily_new_d.py

