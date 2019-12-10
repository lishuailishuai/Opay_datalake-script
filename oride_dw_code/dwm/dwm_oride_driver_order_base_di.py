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

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 11, 15),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_driver_order_base_di',
                  schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 依赖 ---------------------------------------##

# 依赖前一天分区
dwm_oride_order_base_di_prev_day_task = UFileSensor(
    task_id='dwm_oride_order_base_di_prev_day_task',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_order_base_di/country_code=NG",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwm_oride_driver_order_base_di"
hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    msg = [
        {"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
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


def dwm_oride_driver_order_base_di_sql_task(ds):
    HQL = '''

    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    insert overwrite table  oride_dw.{table} partition(country_code,dt)
    select driver_id,
           city_id,
           product_id, 
           --下单业务线
           
           sum(is_request) as driver_request_order_cnt,
           -- 司机接单量（理论和应答量一样）
           
           sum(is_finish) as driver_finish_order_cnt,
           -- 司机完单量
           --判断该司机是否当日完单司机，需要限定driver_finish_order_cnt>=1
           
           sum(is_finished_pay) as driver_finished_pay_order_cnt,
           -- 司机支付完单量	
           
           sum(if(is_finish=1,price,0)) as driver_finish_price,
           --司机完单gmv
           
           sum(billing_order_dur) as driver_billing_dur,
           --司机计费时长，和完单计费时长差异在于有些订单状态4变6
           
           sum(order_service_dur) as driver_service_dur,
           --司机服务时长
           
           sum(finished_order_dur) as driver_finished_dur,
           --司机支付完单做单时长
           
           sum(cannel_pick_order_dur) as driver_cannel_pick_dur,
           --司机当天订单被取消时长,该表不可以用于计算司机在线时长
           
           country_code,
           
           dt
       
        from oride_dw.dwm_oride_order_base_di   --该表统计的是司机和订单相关的指标，但只有接单后的指标司机主题可以分纯业务线，接单前不可分
        where dt='{pt}'
        group by driver_id,
               city_id,
               product_id,
               country_code,
               dt;
'''.format(
        pt=ds,
        table=table_name,
        now_day='{{macros.ds_add(ds, +1)}}',
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
    _sql = "\n" + cf.alter_partition() + "\n" + dwm_oride_driver_order_base_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 生产success
    cf.touchz_success()

dwm_oride_driver_order_base_di_task = PythonOperator(
    task_id='dwm_oride_driver_order_base_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}'
    },
    dag=dag
)

dwm_oride_order_base_di_prev_day_task >> dwm_oride_driver_order_base_di_task
