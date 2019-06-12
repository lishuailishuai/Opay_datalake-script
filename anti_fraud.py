import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from anti_fraud.push import not_pay_push, abnormal_push

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['lichang.zhang@opay-inc.com', 'zhuohua.chen@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_push_task',
    schedule_interval="00 08 * * *",
    catchup=False,
    default_args=args)

not_pay_push_task = PythonOperator(
    task_id='not_pay_push',
    python_callable=not_pay_push,
    dag=dag,
    provide_context=True,
    op_kwargs={"env": "test"}
)

abnormal_push_task = PythonOperator(
    task_id='abnormal_push',
    python_callable=abnormal_push,
    dag=dag,
    provide_context=True,
    op_kwargs={"env": "test"}
)

abnormal_push_task.set_upstream(not_pay_push_task)
