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
from plugins.CountriesPublicFrame import CountriesPublicFrame
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2020, 3, 13),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('app_oride_driver_funnel_d',
                  schedule_interval="45 01 * * *",
                  default_args=args)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "app_oride_driver_funnel_d"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 依赖 ---------------------------------------##
dwm_oride_driver_base_df_prev_day_task = OssSensor(
        task_id='dwm_oride_driver_base_df_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_df/country_code=NG",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
)

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"dag": dag, "db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##

def app_oride_driver_funnel_d_sql_task(ds):
    HQL = '''
    SET hive.exec.parallel=true;
    SET hive.exec.dynamic.partition=true;
    SET hive.exec.dynamic.partition.mode=nonstrict;
    insert overwrite table {db}.{table} partition(country_code,dt)
    SELECT city_id,
           product_id,
           newest_driver_version , --司机端最新版本号
         sum(if(fault<6,1,0)) AS survival_driver_num, --当天存活司机数
         sum(is_td_online) AS online_driver_num, --当天在线司机数
         sum(is_td_broadcast) AS broadcast_driver_num, --被播单司机数（算法push节点）
         sum(is_td_succ_broadcast) AS succ_broadcast_driver_num, --被成功播单司机数（算法push节点）
         sum(is_td_accpet_show) AS push_accept_show_driver_num, --被推送到司机端司机数（司机端push_show和accept_show点）
         sum(is_td_accpet_click) AS accpet_click_driver_num, --被司机接单司机数（司机端click点）
         sum(is_td_request) AS request_driver_num, --接单或应答司机数（订单表）
         sum(is_td_finish) AS finish_driver_num, --完单司机数
         sum(is_td_sign) AS new_sign_driver_num, --新增注册司机数
         sum(push_times) AS driver_push_times, --司机被播单总次数（算法push节点）
         sum(succ_push_times) AS driver_succ_push_times, --司机被成功播单总次数（算法push节点）
         sum(driver_show_order_times) AS driver_push_accept_show_times, --司机端被推单次数（司机端push_show和accept_show点）
         sum(driver_click_order_times) AS driver_click_show_times, --司机端被接单次数（司机端click点）
         sum(push_order_cnt) AS driver_push_order_cnt, --司机被播单数量（算法push节点）
         sum(succ_push_order_cnt) AS driver_succ_push_order_cnt, --司机被成功播单数量（算法push节点）
         sum(driver_show_order_cnt) driver_push_accept_show_order_cnt, --司机端被推单量（司机端push_show和accept_show点）
         sum(driver_click_order_cnt) AS driver_click_show_order_cnt, --司机端司机接单量（司机端click点）
         sum(driver_request_order_cnt) AS driver_request_order_cnt, --司机接单或者应答单量（订单表）
         sum(driver_finish_order_cnt) AS driver_finish_order_cnt, --司机完单量
         sum(driver_finished_pay_order_cnt) AS driver_finished_pay_order_cnt, --司机支付完单量
         country_code AS country_code,
         '{pt}' AS dt
        FROM
          (SELECT *
           FROM oride_dw.dwm_oride_driver_base_df
           WHERE dt='{pt}') dri
        GROUP BY city_id,
                 product_id,
                 newest_driver_version, --司机版本号
         country_code;
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
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
    _sql = "\n" + cf.alter_partition() + "\n" + app_oride_driver_funnel_d_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据，如果数据不能为0
    # check_key_data_cnt_task(ds)

    # 生产success
    cf.touchz_success()


app_oride_driver_funnel_d_task = PythonOperator(
    task_id='app_oride_driver_funnel_d_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_driver_base_df_prev_day_task >> app_oride_driver_funnel_d_task