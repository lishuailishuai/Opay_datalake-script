import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable

args = {
    'owner': 'root',
    'start_date': datetime(2019, 4, 20),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'ofood_hourly',
    schedule_interval="15 * * * *",
    default_args=args)

insert_ofood_dau = HiveOperator(
    hql="""
        insert overwrite table ofood_dau
        select
            dt,
            count(distinct uid)
        FROM
            ofood_source.user_login
        WHERE dt >= '2019-04-20'
        GROUP BY
            dt
    """,
    schema='dashboard',
    task_id='insert_ofood_dau',
    dag=dag)

insert_ofood_dnu = HiveOperator(
    hql="""
        insert overwrite table ofood_dau
        select
            dt,
            count(distinct uid)
        FROM
            ofood_source.user_register
        WHERE dt >= '2019-04-20'
        GROUP BY
            dt
    """,
    schema='dashboard',
    task_id='insert_ofood_dnu',
    dag=dag)


insert_ofood_order_sum = HiveOperator(
    hql="""
        insert overwrite table ofood_order_sum
        select
            dt,
            count(distinct orderid)
        FROM
            ofood_source.user_orders
        WHERE dt >= '2019-04-20'
        GROUP BY
            dt
    """,
    schema='dashboard',
    task_id='insert_ofood_dnu',
    dag=dag)


IMPALA_QUERY = """
    REFRESH dashboard.ofood_dau;
    REFRESH dashboard.ofood_dau;
    REFRESH dashboard.ofood_order_sum;
"""

refresh_impala = BashOperator(
    task_id='refresh_impala',
    bash_command='impala-shell -i "{impala_url}:21000" -q "{impala_query}"'.format(impala_url=Variable.get("IMPALA_URL"), impala_query=IMPALA_QUERY),
    dag=dag)