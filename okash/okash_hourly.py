import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors import UFileSensor

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 8, 8),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'okash_hourly',
    schedule_interval="10 * * * *",
    default_args=args)

check_client_ufile=UFileSensor(
    task_id='check_client_ufile',
    filepath='okash/okash/{table}/dt={{{{ ds }}}}/hour={{{{ execution_date.strftime("%H") }}}}'.format(table='client'),
    bucket_name='okash',
    dag=dag)

add_client_partitions = HiveOperator(
    task_id='add_client_partitions',
    hql="""
            ALTER TABLE ods_log_client_hi ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
    """,
    schema='okash_dw',
    dag=dag)

check_client_ufile >> add_client_partitions