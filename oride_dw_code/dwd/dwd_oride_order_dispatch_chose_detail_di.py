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

dag = airflow.DAG('dwd_oride_order_dispatch_chose_detail_di',
                  schedule_interval="10 00 * * *",
                  default_args=args,
                  catchup=False)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 30',
    dag=dag)
##----------------------------------------- 变量 ---------------------------------------##

db_name = "oride_dw"
table_name = "dwd_oride_order_dispatch_chose_detail_di"

##----------------------------------------- 依赖 ---------------------------------------##
#获取变量
code_map=eval(Variable.get("sys_flag"))

#判断ufile(cdh环境)
if code_map["id"].lower()=="ufile":

    # 依赖前一天分区
    dependence_dispatch_tracker_server_magic_prev_day_task = HivePartitionSensor(
        task_id="dependence_dispatch_tracker_server_magic_prev_day_task",
        table="dispatch_tracker_server_magic",
        partition="dt='{{ ds }}' and hour='23'",
        schema="oride_source",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name

else:
    print("成功")
    dependence_dispatch_tracker_server_magic_prev_day_task = HivePartitionSensor(
        task_id="dependence_dispatch_tracker_server_magic_prev_day_task",
        table="dispatch_tracker_server_magic",
        partition="dt='{{ ds }}' and hour='23'",
        schema="oride_source",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name


##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, dag, **op_kwargs):
    dag_ids = dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table": "{dag_name}".format(dag_name=dag_ids),
         "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "3000"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)


task_timeout_monitor = PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##




def dwd_oride_order_dispatch_chose_detail_di_sql_task(ds):
    HQL = '''
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT OVERWRITE TABLE {db}.{table} partition(country_code,dt)
    SELECT  get_json_object(event_values,'$.city_id')                AS city_id --下单时所在城市 
           ,get_json_object(event_values,'$.order_id')               AS order_id --订单ID 
           ,get_json_object(event_values,'$.user_id')                AS passenger_id --乘客ID 
           ,driver_id --司机ID 
           ,get_json_object(event_values,'$.round')                  AS order_round --订单轮数 
           ,get_json_object(event_values,'$.config_id')              AS config_id --派单配置的id 
           ,get_json_object(event_values,'$.timestamp')              AS log_timestamp --埋点时间 
           ,cast(get_json_object(event_values,'$.serv_type') AS int) AS product_id --业务线ID 
           ,get_json_object(event_values,'$.is_multiple')            AS is_multiple --是否播多单 
           ,null                                                     AS order_id_multiple --多单订单 
           ,'nal'                                                    AS country_code 
           ,dt
    FROM oride_source.dispatch_tracker_server_magic lateral view explode 
    (split(substr(get_json_object(event_values, '$.driver_ids'),2,length(get_json_object(event_values, '$.driver_ids'))-2),',') 
    ) driver_ids AS driver_id
    WHERE dt = '{pt}' 
    AND event_name='dispatch_chose_driver' 
    AND (get_json_object(event_values, '$.is_multiple') is null or lower(get_json_object(event_values, '$.is_multiple'))='false')  
    UNION ALL
    SELECT  get_json_object(event_values,'$.city_id')                AS city_id --下单时所在城市 
           ,get_json_object(event_values,'$.order_id')               AS order_id --订单ID 
           ,get_json_object(event_values,'$.user_id')                AS passenger_id --乘客ID 
           ,driver_id --司机ID 
           ,get_json_object(event_values ,'$.round')                 AS order_round --订单轮数 
           ,get_json_object(event_values,'$.config_id')              AS config_id --派单配置的id 
           ,get_json_object(event_values,'$.timestamp')              AS log_timestamp --埋点时间 
           ,cast(get_json_object(event_values,'$.serv_type') AS int) AS product_id --业务线ID 
           ,get_json_object(event_values,'$.is_multiple')            AS is_multiple --是否播多单 
           ,order_id1                                                AS order_id_multiple --多单订单 
           ,'nal'                                                    AS country_code 
           ,dt
    FROM oride_source.dispatch_tracker_server_magic lateral view explode 
    (split(substr(get_json_object(event_values, '$.driver_ids'),2,length(get_json_object(event_values, '$.driver_ids'))-2),',') 
    ) driver_ids AS driver_id lateral view explode(split(substr(get_json_object(event_values, '$.order_list'),2,length(get_json_object(event_values, '$.order_list'))-2),',')) order_list AS order_id1
    WHERE dt = '{pt}' 
    AND event_name='dispatch_chose_driver' 
    AND lower(get_json_object(event_values, '$.is_multiple'))='true'  

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
    _sql = dwd_oride_order_dispatch_chose_detail_di_sql_task(ds)

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



dwd_oride_order_dispatch_chose_detail_di_task = PythonOperator(
    task_id='dwd_oride_order_dispatch_chose_detail_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

dependence_dispatch_tracker_server_magic_prev_day_task >> \
sleep_time >> \
dwd_oride_order_dispatch_chose_detail_di_task
