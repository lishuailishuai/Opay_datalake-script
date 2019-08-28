import airflow
from airflow.operators.hive_operator import HiveOperator
from datetime import datetime, timedelta
from airflow.sensors import UFileSensor

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 7, 29),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_ods_log_skyeye',
    schedule_interval="00 07 * * *",
    default_args=args)

'''
源数据表
'''
#

table_list = [
    "driver",
    "order",
    "passenge"
]
for table in table_list:
    check_ufile=UFileSensor(
        task_id='check_ufile_{}'.format(table),
        filepath='oride-research/tags/{table}_tags/dt={{{{ ds }}}}/upload_success.txt'.format(table=table),
        bucket_name='opay-datalake',
        timeout=86400,
        dag=dag)

    # add partitions
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(table),
        hql='''
                ALTER TABLE oride_dw.`ods_log_oride_{table}_skyeye_di` ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
            '''.format(table=table),
        schema='oride_dw',
        dag=dag)

    check_ufile >> add_partitions
