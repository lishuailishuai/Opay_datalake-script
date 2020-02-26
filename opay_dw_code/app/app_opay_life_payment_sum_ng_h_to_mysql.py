# -*- coding: utf-8 -*-
"""
平台司机数据2019-08-31
"""
import airflow
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.connection_helper import get_hive_cursor, get_db_conn
from datetime import datetime, timedelta
from airflow.operators.impala_plugin import ImpalaOperator
import re, sys
import logging
from utils.validate_metrics_utils import *
import time
from datetime import datetime, timedelta
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
from airflow.sensors import OssSensor
from plugins.CountriesPublicFrame_dev import CountriesPublicFrame_dev
import logging
from airflow.models import Variable
import requests
import os

args = {
    'owner': 'lijialong',
    'start_date': datetime(2020, 2, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_opay_life_payment_sum_ng_h_to_mysql',
    schedule_interval="50 * * * *",
    default_args=args
)

##----------------------------------- 变量 ----------------------------------##
db_name = "opay_dw"
table_name = "app_opay_life_payment_sum_ng_h"
hdfs_path = "oss://opay-datalake/opay/opay_dw/" + table_name
config = eval(Variable.get("opay_time_zone_config"))
time_zone = config['NG']['time_zone']
mysql_table = 'opay_dw.app_opay_life_payment_sum_ng_h'


##----------------------------------依赖数据源------------------------------##



### 检查当前小时的依赖
app_opay_life_payment_sum_ng_h_check_task = OssSensor(
    task_id='app_opay_life_payment_sum_ng_h_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/app_opay_life_payment_sum_ng_h",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=0),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=0)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

### 检查上一个小时的依赖
app_opay_life_payment_sum_ng_h_pre_check_task = OssSensor(
    task_id='app_opay_life_payment_sum_ng_h_pre_check_task',
    bucket_key='{hdfs_path_str}/country_code=NG/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="opay/opay_dw/app_opay_life_payment_sum_ng_h",
        pt='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%Y-%m-%d")}}}}'.format(
            time_zone=time_zone, gap_hour=-1),
        hour='{{{{(execution_date+macros.timedelta(hours=({time_zone}+{gap_hour}))).strftime("%H")}}}}'.format(
            time_zone=time_zone, gap_hour=-1)
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


##------------------------------------ SQL --------------------------------##

# 从hive读取数据
def get_data_from_hive(ds, execution_date,  **op_kwargs):
    # ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    hql = '''
        SELECT 
            create_date_hour,  
            sub_consume_scenario,      
            recharge_service_provider,     
            originator_type,      
            originator_role,         
            order_status,        
            order_cnt,                
            order_amt,                
            country_code,               
            dt,           
            hour
        from opay_dw.app_opay_life_payment_sum_ng_h
        where 
        country_code = 'NG'
    and concat(dt,' ',hour) >= date_format(default.localTime("{config}", 'NG', '{v_date}', -1), 'yyyy-MM-dd HH')
    and concat(dt,' ',hour) <= date_format(default.localTime("{config}", 'NG', '{v_date}', 0), 'yyyy-MM-dd HH')
        
    '''.format(
        pt=ds,
        v_date= execution_date.strftime("%Y-%m-%d %H:%M:%S"),
        table=table_name,
        db=db_name,
        config=config
    )

    logging.info(hql)
    hive_cursor = get_hive_cursor()
    hive_cursor.execute(hql)
    hive_data = hive_cursor.fetchall()

    mysql_conn = get_db_conn('mysql_bi')
    mcursor = mysql_conn.cursor()

    __data_only_mysql(
        mcursor,
        "dt='{pt}' AND hour='{hour}'".format(pt=ds, hour=(execution_date+airflow.macros.timedelta(hours=time_zone)).strftime("%H"))
    )

    __data_to_mysql(
        mcursor,
        hive_data,
        [
            'create_date_hour',
            'sub_consume_scenario',
            'recharge_service_provider',
            'originator_type',
            'originator_role',
            'order_status',
            'order_cnt',
            'order_amt',
            'country_code',
            'dt',
            'hour',
        ],
        'dt=VALUES(dt)'
    )

    hive_cursor.close()
    mcursor.close()


# 数据集写入mysql前删除之前数据
def __data_only_mysql(conn, column):
    isql = 'delete from {table} {where_dt} ;'.format(
        table=mysql_table,
        where_dt='where' + ' ' + column
    )
    try:
        logging.info(isql)
        conn.execute(isql)
    except BaseException as e:
        logging.info(e)
        sys.exit(1)
        return


# 数据集写入mysql
def __data_to_mysql(conn, data, column, update=''):
    isql = 'insert into {table} ({columns})'.format(
        table=mysql_table,
        columns=','.join(column)
    )
    esql = '{0} values {1} on duplicate key update {2}'
    sval = ''
    cnt = 0
    try:
        for (create_date_hour,sub_consume_scenario,recharge_service_provider,
            originator_type,originator_role,order_statu,order_cnt,
            order_amt,country_code,dt,hour) in data:

            row = [create_date_hour,sub_consume_scenario,recharge_service_provider,
            originator_type,originator_role,order_statu,order_cnt,
            order_amt,country_code,dt,hour
            ]
            if sval == '':
                sval = '(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            else:
                sval += ',(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            cnt += 1
            if cnt >= 1000:
                logging.info(esql.format(isql, sval, update))
                logging.info('mysql insert rows:%d', cnt)
                conn.execute(esql.format(isql, sval, update))
                cnt = 0
                sval = ''

        if cnt > 0 and sval != '':
            logging.info(esql.format(isql, sval, update))
            logging.info('mysql insert rows:%d', cnt)
            conn.execute(esql.format(isql, sval, update))

    except BaseException as e:
        logging.info(e)
        sys.exit(1)
        return


get_data_from_hive_task = PythonOperator(
    task_id='get_data_from_hive_task',
    python_callable=get_data_from_hive,
    provide_context=True,
    op_kwargs={
        'v_execution_date': '{{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}',
        'v_execution_day': '{{execution_date.strftime("%Y-%m-%d")}}',
        'v_execution_hour': '{{execution_date.strftime("%H")}}',
        'owner': '{{owner}}'
    },
    dag=dag
)


app_opay_life_payment_sum_ng_h_check_task >> get_data_from_hive_task
app_opay_life_payment_sum_ng_h_pre_check_task >> get_data_from_hive_task