# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.hive_operator import HiveOperator
from utils.connection_helper import get_hive_cursor, get_db_conn, get_pika_connection, get_redis_connection
from airflow.hooks.hive_hooks import HiveCliHook
from airflow import macros
import logging
from airflow.models import Variable
import pandas as pd
import io
import requests
import os
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *
from airflow.operators.bash_operator import BashOperator
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from plugins.TaskTouchzSuccess import TaskTouchzSuccess

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 10, 10),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'ods_oride_log_driver_timerange', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 

##----------------------------------------- 变量 ---------------------------------------##

db_name="oride_dw_ods"
table_name = "ods_log_oride_driver_timerange"
hdfs_path = "ufile://opay-datalake/oride/oride_dw_ods/" + table_name


##----------------------------------------- 脚本 ---------------------------------------##


KeyDriverOrderTime = "driver:ort:%d:%s"

#键 algo:driver:online:%d:%s 司机id 时间yyyymmdd
KeyDriverOnlineTime = "algo:driver:online:%d:%s"

get_driver_id = '''
select max(id) from oride_data.data_driver
'''
insert_timerange = '''
replace into bi.driver_timerange (`Daily`,`driver_id`,`driver_onlinerange`,`driver_freerange`) values (%s,%s,%s,%s)
'''


def get_driver_online_time(ds, **op_kwargs):
    dt = op_kwargs["ds_nodash"]
    redis = get_redis_connection('pika_85')
    conn = get_db_conn('mysql_oride_data_readonly')
    mcursor = conn.cursor()
    mcursor.execute(get_driver_id)
    result = mcursor.fetchone()
    conn.commit()
    mcursor.close()
    conn.close()
    rows = []
    res = []
    for i in range(1, result[0] + 1):
        online_time_dict = redis.hgetall(KeyDriverOnlineTime % (i, dt))
        if online_time_dict:
            online_time=0
            order_time=0
            for value in  online_time_dict.values():
                tmp_list = str(value, 'utf-8').split('|')
                online_time += int(tmp_list[0])*60
                order_time += int(tmp_list[1])*60

            #print('driver_id:%d, online_time_total:%d' % (i, online_time))
            free_time = online_time - order_time
            rows.append('(' + str(i) + ',' + str(online_time) + ',' + str(free_time) + ')')
    if rows:
        query = """
            INSERT OVERWRITE TABLE oride_dw_ods.{tab_name} PARTITION (dt='{dt}')
            VALUES {value}
        """.format(dt=ds, value=','.join(rows),tab_name=table_name)
        logging.info('import_driver_online_time run sql:%s' % query)
        hive_hook = HiveCliHook()
        hive_hook.run_cli(query)
        # insert bi mysql
        # conn = get_db_conn('mysql_bi')
        # mcursor = conn.cursor()
        # mcursor.executemany(insert_timerange, res)
        # conn.commit()
        #mcursor.close()
        #conn.close()

import_ods_log_oride_driver_timerange = PythonOperator(
    task_id='import_ods_log_oride_driver_timerange',
    python_callable=get_driver_online_time,
    provide_context=True,
    dag=dag
)

create_ods_log_oride_driver_timerange = HiveOperator(
    task_id='create_ods_log_oride_driver_timerange',
    hql="""
        CREATE EXTERNAL TABLE IF NOT EXISTS {tab_name} (
          driver_id int,
          driver_onlinerange int,
          driver_freerange int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
        LOCATION
        'ufile://opay-datalake/oride/oride_dw_ods/{tab_name}'

    """.format(tab_name=table_name),
    schema='oride_dw_ods',
    dag=dag)


#主流程
def execution_data_task_id(ds,**kargs):

    #生成_SUCCESS
    """
    第一个参数true: 数据目录是有country_code分区。false 没有
    第二个参数true: 数据有才生成_SUCCESS false 数据没有也生成_SUCCESS 

    """
    TaskTouchzSuccess().countries_touchz_success(ds,db_name,table_name,hdfs_path,"false","true")
    
TaskTouchzSuccess_task= PythonOperator(
    task_id='TaskTouchzSuccess_task',
    python_callable=execution_data_task_id,
    provide_context=True,
    dag=dag
)



create_ods_log_oride_driver_timerange >> import_ods_log_oride_driver_timerange>>TaskTouchzSuccess_task