import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator


args = {
    'owner': 'root',
    'start_date': datetime(2019, 4, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'ofood_daily',
    schedule_interval="30 01 * * *",
    default_args=args)


