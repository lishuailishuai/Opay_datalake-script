import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.sensors import OssSensor

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

check_client_file=OssSensor(
    task_id='check_client_file',
    bucket_key='okash/okash/{table}/dt={{{{ ds }}}}/hour={{{{ execution_date.strftime("%H") }}}}'.format(table='client'),
    bucket_name='okash',
    timeout=3600,
    dag=dag)


add_client_partitions = HiveOperator(
    task_id='add_client_partitions',
    hql="""
            ALTER TABLE ods_log_client_hi ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
    """,
    schema='okash_dw',
    dag=dag)

export_to_mysql = HiveToMySqlTransfer(
    task_id='export_to_mysql',
    sql="""
            SELECT
                *,
                from_unixtime(unix_timestamp())
            FROM
                okash_dw.ods_log_client_hi
            WHERE
                dt='{{ ds }}'
                AND hour='{{ execution_date.strftime("%H") }}'
        """,
    mysql_conn_id='mysql_bi',
    mysql_table='okash_dw.ods_log_client_hi',
    dag=dag)

check_client_file >> add_client_partitions >> export_to_mysql