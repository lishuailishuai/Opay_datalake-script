# -*- coding: utf-8 -*-
"""
每小时添加hive表分区
"""
import airflow
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 9, 10),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'opay_source_log',
    schedule_interval="10 * * * *",
    default_args=args
)


def add_partitions(**op_kwargs):
    dt = op_kwargs.get('dt')
    hour = op_kwargs.get('hour')
    hive_cursor = get_hive_cursor()
    hql = '''
        SHOW TABLES IN opay_source '*'
    '''
    logging.info(hql)
    hive_cursor.execute(hql)
    tables = hive_cursor.fetchall()
    for (table, ) in tables:
        sql = '''
            ALTER TABLE opay_source.{table} ADD IF NOT EXISTS PARTITION (country_code='nal', dt='{dt}', hour='{hour}')
        '''.format(
            table=table,
            dt=dt,
            hour=hour
        )
        logging.info(sql)
        hive_cursor.execute(sql)

    hive_cursor.close()


add_partitions_py = PythonOperator(
    task_id='add_partitions_py',
    python_callable=add_partitions,
    provide_context=True,
    op_kwargs={
        "dt": '{{ ds }}',
        "hour": '{{ execution_date.strftime("%H") }}'
    },
    dag=dag
)

add_partitions_py
