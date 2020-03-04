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
import json
import logging
from airflow.models import Variable
import requests
import os
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
from airflow.sensors import OssSensor

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 5, 20),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_driver_accept_order_show_detail_di',
                  schedule_interval="30 00 * * *",
                  default_args=args,
                  )

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name="dwd_oride_driver_accept_order_show_detail_di"

##----------------------------------------- 依赖 ---------------------------------------##

#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":
    # 依赖前一天分区
    dwd_oride_driver_accept_order_show_detail_di_prev_day_task = UFileSensor(
        task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
        filepath='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
            pt='{{ds}}',
            hour='23'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print("成功")
    # 依赖前一天分区
    dwd_oride_driver_accept_order_show_detail_di_prev_day_task = OssSensor(
        task_id='dwd_oride_driver_accept_order_show_detail_di_prev_day_task',
        bucket_key='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
            hdfs_path_str="oride/oride_dw/dwd_oride_client_event_detail_hi/country_code=nal",
            pt='{{ds}}',
            hour='23'
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

    tb = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "1200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##


def dwd_oride_driver_accept_order_show_detail_di_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT OVERWRITE TABLE {db}.{table} partition(country_code,dt)
    SELECT  driver_id --主键（driver_id order_id） 
           ,city_id --定位城市 
           ,product_id --接单司机类型，1=专车，2=快车 
           ,order_id --订单号 
           ,startLat --订单当前纬度 
           ,startLng --订单当前经度 
           ,lat --用户当前纬度 
           ,lng --用户当前经度 
           ,log_timestamp --埋点时间 
           ,isAssign --是否强派单 1强派单 0非强派单 
           ,event_name --事件类型 
           ,'nal'  AS country_code --国家码字段 
           ,'{pt}' AS dt
    FROM 
    (
        SELECT  user_id                                    AS driver_id 
               ,get_json_object(event_value,'$.order_id')  AS order_id 
               ,get_json_object(event_value,'$.city_id')   AS city_id 
               ,get_json_object(event_value,'$.serv_type') AS product_id 
               ,get_json_object(event_value,'$.lat')       AS lat 
               ,get_json_object(event_value,'$.lng')       AS lng 
               ,get_json_object(event_value,'$.startLat')  AS startLat 
               ,get_json_object(event_value,'$.startLng')  AS startLng 
               ,event_time                                 AS log_timestamp 
               ,get_json_object(event_value,'$.isAssign')  AS isAssign --是否强派单 
               ,event_name --事件类型
        FROM oride_dw.dwd_oride_client_event_detail_hi
        WHERE dt='{pt}' 
        AND event_name in('accept_order_show','order_push_show')  
        UNION ALL
        SELECT  user_id                                    AS driver_id 
               ,order_id 
               ,get_json_object(event_value,'$.city_id')   AS city_id 
               ,get_json_object(event_value,'$.serv_type') AS product_id 
               ,get_json_object(event_value,'$.lat')       AS lat 
               ,get_json_object(event_value,'$.lng')       AS lng 
               ,get_json_object(event_value,'$.startLat')  AS startLat 
               ,get_json_object(event_value,'$.startLng')  AS startLng 
               ,event_time                                 AS log_timestamp 
               ,get_json_object(event_value,'$.isAssign')  AS isAssign --是否强派单 
               ,event_name --事件类型
        FROM oride_dw.dwd_oride_client_event_detail_hi lateral view explode 
        (split(substr(get_json_object(event_value, '$.order_ids'),2,length(get_json_object(event_value, '$.order_ids'))-2),',') 
        ) order_ids AS order_id
        WHERE dt='{pt}' 
        AND event_name in('accept_order_show','order_push_show')  
    ) t ;

'''.format(
        pt=ds,
        table=table_name,
        db=db_name
    )
    return HQL


# 熔断数据，如果数据重复，报错
def check_key_data_task(ds):
    cursor = get_hive_cursor()

    # 主键重复校验
    check_sql = '''
    SELECT count(1)-count(distinct (concat(order_id,'_',driver_id))) as cnt
      FROM {db}.{table}
      WHERE dt='{pt}'
      and country_code in ('nal')
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        table=table_name,
        db=db_name
    )

    logging.info('Executing 主键重复校验: %s', check_sql)

    cursor.execute(check_sql)

    res = cursor.fetchone()

    if res[0] > 1:
        flag = 1
        raise Exception("Error The primary key repeat !", res)
        sys.exit(1)
    else:
        flag = 0
        print("-----> Notice Data Export Success ......")

    return flag


# 主流程
def execution_data_task_id(ds, **kargs):
    hive_hook = HiveCliHook()

    # 读取sql
    _sql = dwd_oride_driver_accept_order_show_detail_di_sql_task(ds)

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



dwd_oride_driver_accept_order_show_detail_di_task = PythonOperator(
    task_id='dwd_oride_driver_accept_order_show_detail_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dwd_oride_driver_accept_order_show_detail_di_prev_day_task >>dwd_oride_driver_accept_order_show_detail_di_task
