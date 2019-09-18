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

args = {
        'owner': 'yangmingze',
        'start_date': datetime(2019, 6, 20),
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=2),
        'email': ['bigdata_dw@opay-inc.com'],
        'email_on_failure': True,
        'email_on_retry': False,
} 

dag = airflow.DAG( 'ods_log_oride_driver_timerange', 
    schedule_interval="00 01 * * *", 
    default_args=args,
    catchup=False) 



KeyDriverOnlineTime = "driver:ont:%d:%s"
KeyDriverOrderTime = "driver:ort:%d:%s"

get_driver_id = '''
select max(id) from oride_data.data_driver
'''
insert_timerange = '''
replace into bi.driver_timerange (`Daily`,`driver_id`,`driver_onlinerange`,`driver_freerange`) values (%s,%s,%s,%s)
'''


def get_driver_online_times(ds, **op_kwargs):
    dt = op_kwargs["ds_nodash"]
    redis = get_redis_connection()
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
        online_time = redis.get(KeyDriverOnlineTime % (i, dt))
        order_time = redis.get(KeyDriverOrderTime % (i, dt))
        if online_time is not None:
            if order_time is None:
                order_time = 0
            free_time = int(online_time) - int(order_time)
            res.append([dt + '000000', int(i), int(online_time), int(free_time)])
            rows.append('(' + str(i) + ',' + str(online_time, 'utf-8') + ',' + str(free_time) + ')')
    if rows:
        query = """
            INSERT OVERWRITE TABLE oride_dw_ods.ods_log_oride_driver_timerange PARTITION (dt='{dt}')
            VALUES {value}
        """.format(dt=ds, value=','.join(rows))
        logging.info('import_driver_online_time run sql:%s' % query)
        hive_hook = HiveCliHook()
        hive_hook.run_cli(query)
        # insert bi mysql
        conn = get_db_conn('mysql_bi')
        mcursor = conn.cursor()
        mcursor.executemany(insert_timerange, res)
        conn.commit()
        mcursor.close()
        conn.close()


def get_driver_online_time(ds, **op_kwargs):
    dt = op_kwargs["ds_nodash"]
    redis = get_redis_connection()
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
        online_time = redis.get(KeyDriverOnlineTime % (i, dt))
        order_time = redis.get(KeyDriverOrderTime % (i, dt))
        if online_time is not None:
            if order_time is None:
                order_time = 0
            free_time = int(online_time) - int(order_time)
            res.append([dt + '000000', int(i), int(online_time), int(free_time)])
            rows.append('(' + str(i) + ',' + str(online_time, 'utf-8') + ',' + str(free_time) + ')')
    if rows:
        query = """
            INSERT OVERWRITE TABLE oride_dw_ods.ods_log_oride_driver_timerange PARTITION (dt='{dt}')
            VALUES {value}
        """.format(dt=ds, value=','.join(rows))
        #logging.info('import_driver_online_time run sql:%s' % query)
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
        CREATE EXTERNAL TABLE IF NOT EXISTS ods_log_oride_driver_timerange (
          driver_id int,
          driver_onlinerange int,
          driver_freerange int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
        LOCATION
        'ufile://opay-datalake/oride/oride_dw_ods/ods_log_oride_driver_timerange'

    """,
    schema='oride_dw_ods',
    dag=dag)

create_ods_log_oride_driver_timerange >> import_ods_log_oride_driver_timerange