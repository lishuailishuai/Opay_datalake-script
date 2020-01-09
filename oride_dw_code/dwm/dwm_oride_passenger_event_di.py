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
from airflow.sensors import OssSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lili.chen',
    'start_date': datetime(2019, 12, 10),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwm_oride_passenger_event_di',
                  schedule_interval="00 02 * * *",
                  default_args=args,
                  catchup=False)

##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwm_oride_passenger_event_di"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖前一天分区
    dwm_oride_passenger_event_hi_prev_day_task = UFileSensor(
        task_id='dwm_oride_passenger_event_hi_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
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
    dwm_oride_passenger_event_hi_prev_day_task = OssSensor(
        task_id='dwm_oride_passenger_event_hi_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "2400"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)



task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)


##----------------------------------------- 脚本 ---------------------------------------##


def dwm_oride_passenger_event_di_sql_task(ds):
    HQL='''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

INSERT overwrite TABLE oride_dw.{table} partition(country_code,dt)
select sum(if(event_name='oride_show',1,0)) as act_num, --乘客端打开页面活跃量
       sum(if(event_name='choose_end_point_click',1,0)) as choose_end_point_num, --地址选择次数
       sum(if(event_name='request_a_ride_show',1,0)) as valuation_num,--估价次数 
       sum(if(event_name='request_a_ride_click',1,0)) as request_order_cnt,  --乘客埋点端统计下单量
       count(distinct if(event_name='oride_show',user_id,null)) as act_user_num,--乘客端打开页面乘客数即活跃乘客数
       count(distinct if(event_name='choose_end_point_click',user_id,null)) as choose_end_user_num,--选择终点乘客数
       count(distinct if(event_name='request_a_ride_show',user_id,null)) as arrive_valuation_user_num, --到达估价页面乘客数 
       count(distinct if(event_name='request_a_ride_click',user_id,null)) as request_user_cnt,  --乘客埋点下单乘客数
       'nal' as country_code,
       '{pt}' as dt
from (select * 
from oride_dw.dwd_oride_client_event_detail_hi
where dt='{pt}' and app_version>='4.4.405' 
and from_unixtime(cast(event_time as int),'yyyy-MM-dd')='{pt}' 
and event_name in('oride_show','choose_end_point_click','request_a_ride_show','request_a_ride_click')
--group by event_name,
     --  user_id,
      -- event_time
       ) t;
 '''.format(
        pt=ds,
        table=table_name,
        db=db_name
        )
    return HQL


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwm_oride_passenger_event_di_sql_task(ds)

    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    #check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")


dwm_oride_passenger_event_di_task = PythonOperator(
    task_id='dwm_oride_passenger_event_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwm_oride_passenger_event_hi_prev_day_task >> dwm_oride_passenger_event_di_task