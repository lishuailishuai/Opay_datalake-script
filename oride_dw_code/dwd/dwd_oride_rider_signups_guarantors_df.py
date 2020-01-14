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
from airflow.sensors import OssSensor

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 10, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_rider_signups_guarantors_df',
                  schedule_interval="20 02 * * *",
                  default_args=args,
                  catchup=False)
##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw"
table_name = "dwd_oride_rider_signups_guarantors_df"


##----------------------------------------- 依赖 ---------------------------------------##
# 获取变量
code_map = eval(Variable.get("sys_flag"))

# 判断ufile(cdh环境)
if code_map["id"].lower() == "ufile":

    # 依赖前一天分区
    ods_sqoop_mass_rider_signups_guarantors_df_task = UFileSensor(
        task_id='ods_sqoop_mass_rider_signups_guarantors_df_task',
        filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/opay_spread/rider_signups_guarantors",
            pt='{{ds}}'
        ),
        bucket_name='opay-datalake',
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    # 路径
    hdfs_path = "ufile://opay-datalake/oride/oride_dw/" + table_name
else:
    print ("成功")

    ods_sqoop_mass_rider_signups_guarantors_df_task = OssSensor(
        task_id='ods_sqoop_mass_rider_signups_guarantors_df_task',
        bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
            hdfs_path_str="oride_dw_sqoop/opay_spread/rider_signups_guarantors",
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

    tb = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=dag_ids), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_rider_signups_guarantors_df_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table oride_dw.{table} partition(country_code,dt)

        SELECT
            id, 
            rider_id as driver_id,--骑手id, 
            name,--'担保人姓名', 
            gender,--'1-male,2-female', 
            country,--'国家', 
            state,--'州', 
            city,--'城市', 
            address,--'地址', 
            address_photo,--'', 
            address_status,--'', 
            address_status_note,--'地址验证未通过原因', 
            mobile,--'', 
            n_passport,--'未验证护照照片地址', 
            y_passport,--'验证护照照片.', 
            passport_status,--'', 
            passport_status_note,--'证件验证未通过原因', 
            address_admin_id,--'地址验证人ID.', 
            address_admin_time, 
            passport_admin_id,--'护照验证人ID.', 
            passport_admin_time,--'passport pass time.', 
            note,--'审核备注', 
            update_time, 
            create_time,
            'nal' as country_code,
            '{pt}' as dt
        FROM
            oride_dw_ods.ods_sqoop_mass_rider_signups_guarantors_df
        WHERE
            dt='{pt}'
        ;
'''.format(
        pt=ds,
        now_day='{{macros.ds_add(ds, +1)}}',
        now_hour='{{ execution_date.strftime("%H") }}',
        table=table_name,
        db=db_name
        )
    return HQL


#主流程
def execution_data_task_id(ds,**kargs):

    hive_hook = HiveCliHook()

    #读取sql
    _sql=dwd_oride_rider_signups_guarantors_df_sql_task(ds)

    logging.info('Executing: %s', _sql)

    #执行Hive
    hive_hook.run_cli(_sql)

    #熔断数据
    #check_key_data_task(ds)

    #生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"true","true")
    
dwd_oride_rider_signups_guarantors_df_task= PythonOperator(
    task_id='dwd_oride_rider_signups_guarantors_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)


ods_sqoop_mass_rider_signups_guarantors_df_task >> dwd_oride_rider_signups_guarantors_df_task
