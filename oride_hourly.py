import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_hourly',
    schedule_interval="20 * * * *",
    default_args=args)

add_partitions = HiveOperator(
    task_id='add_partitions',
    hql="""
            ALTER TABLE driver_action ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE moto_locations ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE order_locations ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_action ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_login ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_order ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_payment ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='oride_source',
    dag=dag)

