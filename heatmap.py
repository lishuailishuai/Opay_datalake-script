import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from heatmap.heatmap import generate_heat_map

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['lichang.zhang@opay-inc.com', 'zhuohua.chen@opay-inc.com'],
    'email_on_failure': True,
}

dag = airflow.DAG(
    'heat_map',
    schedule_interval="*/5 * * * *",
    default_args=args,
    catchup=False,
)

import_table_task = PythonOperator(
    task_id='heat_map',
    python_callable=generate_heat_map,
    dag=dag,
    provide_context=True,
)
