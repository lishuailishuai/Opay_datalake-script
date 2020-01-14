
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
from airflow.sensors import OssSensor
import json
import logging
from airflow.models import Variable
import requests
import os

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2020, 12, 12),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'dwd_oride_algo_driver_score_init_info_di', 
    schedule_interval="10 02 * * *", 
    default_args=args,
    catchup=False) 


##----------------------------------------- 依赖 ---------------------------------------## 


ods_sqoop_algo_driver_score_init_info_di_tesk = OssSensor(
    task_id='ods_sqoop_algo_driver_score_init_info_di_tesk',
    bucket_key="{hdfs_path_str}/dt={pt}/_SUCCESS".format(
        hdfs_path_str="oride_dw_sqoop_di/algorithm/driver_score_init_info",
        pt="{{ds}}"
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##----------------------------------------- 变量 ---------------------------------------## 

db_name="oride_dw"
table_name="dwd_oride_algo_driver_score_init_info_di"
hdfs_path="oss://opay-datalake/oride/oride_dw/dwd_oride_algo_driver_score_init_info_di"


##----------------------------------------- 任务超时监控 ---------------------------------------## 

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    msg = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=NG/dt={pt}".format(pt=ds), "timeout": "800"}
    ]

    TaskTimeoutMonitor().set_task_monitor(msg)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------## 

def dwd_oride_algo_driver_score_init_info_di_sql_task(ds):

    HQL='''

    
    set hive.exec.parallel=true;
    set hive.exec.dynamic.partition.mode=nonstrict;

    INSERT overwrite TABLE oride_dw.dwd_oride_algo_driver_score_init_info_di partition(country_code,dt)

    select

        
id,--未知
driver_id,--未知
city,--未知
serv_type,--未知
group_type,--未知
`_dt`,--未知
create_time,--未知
final_score_target,--未知
first_order_fin_days,--未知
notify_time,--未知
rslt_notify_time,--未知
final_score,--未知
is_new_driver,--未知
'nal' as country_code,
'{pt}' as dt
        
    from oride_dw_ods.ods_sqoop_algo_driver_score_init_info_di
    where dt='{pt}'

    
    '''.format(
        pt=ds,
        now_day=airflow.macros.ds_add(ds, +1),
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name,
        db=db_name
        )
    return HQL


#熔断数据，如果数据重复，报错
# def check_key_data_task(ds):

#     cursor = get_hive_cursor()

#     #主键重复校验
#     check_sql='''
#     SELECT count(1)-count(distinct city_id) as cnt
#       FROM {db}.{table}
#       WHERE dt='{pt}'
#       and country_code in ('NG')
#     '''.format(
#         pt=ds,
#         now_day=airflow.macros.ds_add(ds, +1),
#         table=table_name,
#         db=db_name
#         )

#     logging.info('Executing 主键重复校验: %s', check_sql)

#     cursor.execute(check_sql)

#     res = cursor.fetchone()
 
#     if res[0] >1:
#         flag=1
#         raise Exception ("Error The primary key repeat !", res)
#         sys.exit(1)
#     else:
#         flag=0
#         print("-----> Notice Data Export Success ......")

#     return flag



#主流程
def execution_data_task_id(ds,**kwargs):

    v_date=kwargs.get('v_execution_date')
    v_day=kwargs.get('v_execution_day')
    v_hour=kwargs.get('v_execution_hour')

    hive_hook = HiveCliHook()

    """
        #功能函数
        alter语句: alter_partition
        删除分区: delete_partition
        生产success: touchz_success

        #参数
        第一个参数true: 数据目录是有country_code分区。false 没有
        第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

        #读取sql
        %_sql_task(ds,v_hour)

        第一个参数ds: 天级任务
        第二个参数v_hour: 小时级任务，需要使用

    """

    cf=CountriesPublicFrame("true",ds,db_name,table_name,hdfs_path,"true","true")

    #删除分区
    #cf.delete_partition()

    #拼接SQL

    _sql="\n"+cf.alter_partition()+"\n"+dwd_oride_algo_driver_score_init_info_di_sql_task(ds)

    logging.info('Executing: %s',_sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据，如果数据不能为0
    #check_key_data_cnt_task(ds)

    #熔断数据
    #check_key_data_task(ds)

    #生产success
    cf.touchz_success()

    
dwd_oride_algo_driver_score_init_info_di_task= PythonOperator(
    task_id='dwd_oride_algo_driver_score_init_info_di_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    op_kwargs={
        'v_execution_date':'{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day':'{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour':'{{execution_date.strftime("%H")}}'
    },
    dag=dag
)



ods_sqoop_algo_driver_score_init_info_di_tesk>>dwd_oride_algo_driver_score_init_info_di_task