# -*- coding: utf-8 -*-
"""
每小时添加hive表分区
"""
import airflow
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
import logging

args = {
    'owner': 'yangmingze',
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

add_opay_source_partitions = HiveOperator(
    task_id='add_opay_source_partitions',
    hql="""
            ALTER TABLE binlog_user_order  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');          
            ALTER TABLE client_event  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');                                  
            ALTER TABLE ussd_api  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');                   
            ALTER TABLE ussd_bussiness  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');             
            ALTER TABLE ussd_error  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');                 
            ALTER TABLE ussd_request  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');               
            ALTER TABLE wap_topic_airtime_code  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');     
            ALTER TABLE wap_topic_cash_code_cashin  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}'); 
            ALTER TABLE wap_topic_cash_code_cashout  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE wap_topic_login  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');            
            ALTER TABLE wap_topic_register_code  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');    
            ALTER TABLE wap_topic_send_code  ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');        
            ALTER TABLE wap_topic_transfer_code   ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');   
        """,
    schema='opay_source',
    dag=dag)


# def add_partitions(**op_kwargs):
#     dt = op_kwargs.get('dt')
#     hour = op_kwargs.get('hour')
#     hive_cursor = get_hive_cursor()
#     hql = '''
#         SHOW TABLES IN opay_source '*'
#     '''
#     logging.info(hql)
#     hive_cursor.execute(hql)
#     tables = hive_cursor.fetchall()
#     for (table, ) in tables:
#         sql = '''
#             ALTER TABLE opay_source.{table} ADD IF NOT EXISTS PARTITION ( dt='{dt}', hour='{hour}')
#         '''.format(
#             table=table,
#             dt=dt,
#             hour=hour
#         )
#         logging.info(sql)
#         hive_cursor.execute(sql)

#     hive_cursor.close()


# add_partitions_py = PythonOperator(
#     task_id='add_partitions_py',
#     python_callable=add_partitions,
#     provide_context=True,
#     op_kwargs={
#         "dt": '{{ ds }}',
#         "hour": '{{ execution_date.strftime("%H") }}'
#     },
#     dag=dag
# )

add_opay_source_partitions
