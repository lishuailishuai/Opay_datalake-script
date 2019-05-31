import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from oride_daily_report.query_data import query_data, write_email
from oride_daily_report.import_tables import import_table

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 14),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['lichang.zhang@opay-inc.com', 'zhuohua.chen@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_daily_stat',
    schedule_interval="00 01 * * *",
    catchup=False,
    default_args=args)

import_table_task = PythonOperator(
    task_id='import_table',
    python_callable=import_table,
    dag=dag,
    provide_context=True,
)

query_data_task = PythonOperator(
    task_id='query_data',
    python_callable=query_data,
    dag=dag,
    provide_context=True,
)

query_data_task.set_upstream(import_table_task)

write_email_task = PythonOperator(
    task_id='write_email',
    python_callable=write_email,
    dag=dag,
    provide_context=True,
)

write_email_task.set_upstream(query_data_task)
