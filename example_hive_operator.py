import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'root',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True,
    'depends_on_past': False
}

dag = airflow.DAG(
    'example_hive_operator',
    schedule_interval="@hourly",
    dagrun_timeout=timedelta(minutes=60),
    default_args=args,
    max_active_runs=10)

t1 = HiveOperator(
    hql='show databases',
    schema='default',
    task_id='t1',
    dag=dag)


