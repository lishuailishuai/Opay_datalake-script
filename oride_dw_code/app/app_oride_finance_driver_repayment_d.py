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
from airflow.sensors import UFileSensor
import re,sys
import logging
from utils.validate_metrics_utils import *
import time

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 31),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_oride_finance_driver_repayment_d',
    schedule_interval="30 03 * * *",
    default_args=args
)

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 60',
    dag=dag
)

hive_db = 'oride_dw'
hive_table = 'dwd_oride_finance_driver_repayment_extend_df'

"""
##----------------------------------依赖数据源------------------------------##
"""

#依赖前一天数据是否存在
dwd_oride_finance_driver_repayment_extend_df_tesk = UFileSensor(
    task_id='dwd_oride_finance_driver_repayment_extend_df_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_finance_driver_repayment_extend_df/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)

#依赖前一天数据是否存在
dwm_oride_driver_base_di_tesk = UFileSensor(
    task_id='dwm_oride_driver_base_di_tesk',
    filepath='{hdfs_path_str}/dt={pt}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwm_oride_driver_base_di/country_code=nal",
        pt='{{ds}}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)


"""
##------------------------------------ end --------------------------------##
"""

mysql_table = 'oride_assets.oride_assets_finance_driver_repayment'


# 从hive读取数据
def get_data_from_hive(**op_kwargs):
    ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    hql = '''
        SELECT t1.dt,
            NVL(t1.city_id, 0),
            NVL(city_name, ''),
            NVL(t1.driver_id, 0),
            NVL(driver_name, ''),
            NVL(phone_number, ''),
            NVL(t1.product_id, 0),
            CASE
                WHEN balance IS NULL THEN 0
                ELSE balance
            END,
            CASE
                WHEN repayment_all IS NULL THEN 0
                ELSE repayment_all
            END,
            NVL(start_date, ''),
            CASE
                WHEN repayment_amount IS NULL THEN 0
                ELSE repayment_amount
            END,
            NVL(numbers, 0),
            0 AS effective_days,
            NVL(overdue_payment_cnt, 0),
            NVL(last_repayment_time, dt),
            is_td_valid AS today_repayment,
            0 AS status,
            nvl(driver_finish_ord_num,0) AS order_numbers 
        FROM (select * FROM {hive_db}.{hive_table}
        WHERE dt = '{pt}') t1
        
        LEFT OUTER JOIN
            
            (SELECT product_id,
                    city_id,
                    driver_id,
                    driver_finish_ord_num
             FROM oride_dw.dwm_oride_driver_base_di
             WHERE dt = '{pt}') t2 
        ON t1.driver_id=t2.driver_id
          AND t1.city_id=t2.city_id
          AND t1.product_id=t2.product_id
        

    '''.format(
        hive_db=hive_db,
        hive_table=hive_table,
        pt=ds
    )

    logging.info(hql)
    hive_cursor = get_hive_cursor()
    hive_cursor.execute(hql)
    hive_data = hive_cursor.fetchall()

    mysql_conn = get_db_conn('opay_spread_mysql')
    mcursor = mysql_conn.cursor()

    __data_to_mysql(
        mcursor,
        hive_data,
        [
            'day', 'city_id', 'city_name', 'driver_id', 'driver_name', 'driver_mobile', 'driver_type',
            'balance', 'repayment_total_amount', 'start_date', 'repayment_amount', 'total_numbers',
            'effective_days', 'lose_numbers', 'last_back_time', 'today_repayment', 'status','order_numbers'
        ],
        'day=VALUES(day)'
    )

    hive_cursor.close()
    mcursor.close()


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
        for (day, city_id, city_name, driver_id, driver_name, driver_mobile, driver_type,
                balance, repayment_total_amount, start_date, repayment_amount, total_numbers,
                effective_days, lose_numbers, last_back_time, today_repayment, status,order_numbers) in data:

            row = [
                day, city_id, city_name, driver_id, driver_name.replace("'", "\\'"), driver_mobile, driver_type,
                balance, repayment_total_amount, start_date, repayment_amount, total_numbers,
                effective_days, lose_numbers, last_back_time, today_repayment, status,order_numbers
            ]
            if sval == '':
                sval = '(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            else:
                sval += ',(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            cnt += 1
            if cnt >= 1000:
                # logging.info(esql.format(isql, sval, update))
                conn.execute(esql.format(isql, sval, update))
                cnt = 0
                sval = ''

        if cnt > 0 and sval != '':
            # logging.info(esql.format(isql, sval, update))
            conn.execute(esql.format(isql, sval, update))
    except BaseException as e:
        logging.info(e)
        sys.exit(1)
        return


get_data_from_hive_task = PythonOperator(
    task_id='get_data_from_hive_task',
    python_callable=get_data_from_hive,
    provide_context=True,
    dag=dag
)


dwd_oride_finance_driver_repayment_extend_df_tesk>>dwm_oride_driver_base_di_tesk>>sleep_time >> get_data_from_hive_task
