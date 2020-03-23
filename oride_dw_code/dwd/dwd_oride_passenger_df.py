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
from airflow.sensors.web_hdfs_sensor import WebHdfsSensor
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess
import json
import logging
from airflow.models import Variable
import requests
import os
from airflow.sensors import OssSensor

args = {
    'owner': 'lishuai',
    'start_date': datetime(2020, 3, 22),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG('dwd_oride_passenger_df',
                  schedule_interval="40 00 * * *",
                  default_args=args,

)
##----------------------------------------- 变量 ---------------------------------------##



ods_binlog_base_data_user_hi_prev_day_task = OssSensor(
    task_id='ods_binlog_base_data_user_hi_prev_day_task',
    bucket_key='{hdfs_path_str}/dt={pt}/hour=23/_SUCCESS'.format(
        hdfs_path_str="oride_binlog/oride_db.oride_data.data_user",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

# 依赖前天分区
dwd_oride_passenger_df_prev_day_tesk = OssSensor(
    task_id='dwd_oride_passenger_df_prev_day_tesk',
    bucket_key='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_passenger_df/country_code=nal",
        pt='{{macros.ds_add(ds, -1)}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

##----------------------------------------- 变量 ---------------------------------------##
db_name="oride_dw"
table_name = "dwd_oride_passenger_df"
hdfs_path = "oss://opay-datalake/oride/oride_dw/" + table_name
##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds,dag,**op_kwargs):

    dag_ids=dag.dag_id

    tb = [
        {"dag":dag,"db": "oride_dw", "table":"{dag_name}".format(dag_name=table_name), "partition": "country_code=nal/dt={pt}".format(pt=ds), "timeout": "600"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)

task_timeout_monitor= PythonOperator(
    task_id='task_timeout_monitor',
    python_callable=fun_task_timeout_monitor,
    provide_context=True,
    dag=dag
)

##----------------------------------------- 脚本 ---------------------------------------##

def dwd_oride_passenger_df_sql_task(ds):

    HQL='''
        SET hive.exec.parallel=TRUE;
        SET hive.exec.dynamic.partition.mode=nonstrict;

        insert overwrite table {db}.{table} partition(country_code,dt)

        SELECT
            nvl(data_user.id,data_user_bef.passenger_id) as passenger_id,--'用户 ID', 
            nvl(data_user.phone_number,data_user_bef.phone_number) as passenger_id,--'手机号' 
            nvl(data_user.first_name,data_user_bef.first_name) as passenger_id,--'名'
            nvl(data_user.last_name,data_user_bef.last_name) as passenger_id,--'性'
            nvl(data_user.promoter_code,data_user_bef.promoter_code) as passenger_id,--'推广员代码'
            nvl(from_unixtime(unix_timestamp(data_user.updated_at)+3600,'yyyy-MM-dd'),data_user_bef.updated_at) as passenger_id,--'最后更新时间', 
            nvl(data_user.opay_id,data_user_bef.opay_id) as passenger_id,--'用户OPAYID'
            'nal' as country_code,
            '{pt}' as dt
        FROM
        (select * 
        from oride_dw.dwd_oride_passenger_df
        where dt='{bef_yes_day}') data_user_bef
        full outer join 
        (
            SELECT 
                * 
            FROM
             (
                SELECT 
                    *,
                     row_number() over(partition by t.id order by t.`__ts_ms` desc) as order_by
                FROM oride_dw_ods.ods_binlog_base_data_user_hi  t
                WHERE concat_ws(' ',dt,hour) BETWEEN '{bef_yes_day} 23' AND '{pt} 22'--取昨天1天数据与今天早上00数据
             ) t1
            where t1.`__deleted` = 'false' and t1.order_by = 1
        ) data_user
        on data_user_bef.passenger_id=data_user.id;


'''.format(
        pt=ds,
        bef_yes_day=airflow.macros.ds_add(ds, -1),
        table=table_name,
        db=db_name
    )
    return HQL

#熔断数据，如果数据重复，报错
def check_key_data_task(ds):

    cursor = get_hive_cursor()

    #主键重复校验
    check_sql='''
    select count(1)-count(distinct passenger_id) as cnt
    from {db}.{table}
    where dt='{pt}'
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

#主流程
def execution_data_task_id(ds,**kargs):

    hive_hook = HiveCliHook()

    #读取sql
    _sql=dwd_oride_passenger_df_sql_task(ds)
    logging.info('Executing: %s', _sql)

    # 执行Hive
    hive_hook.run_cli(_sql)

    # 熔断数据
    check_key_data_task(ds)

    # 生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds, db_name, table_name, hdfs_path, "true", "true")

dwd_oride_passenger_df_task=PythonOperator(
    task_id='dwd_oride_passenger_df_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)

ods_binlog_base_data_user_hi_prev_day_task >>  dwd_oride_passenger_df_task
dwd_oride_passenger_df_prev_day_tesk >> dwd_oride_passenger_df_task