import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.models import Variable
from impala.dbapi import connect
from airflow.operators.python_operator import PythonOperator

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

user_login_add_partitions = HiveOperator(
    task_id='user_login_add_partitions',
    hql="""
            ALTER TABLE user_login ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        """,
    schema='ofood_source',
    dag=dag)

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

user_register_add_partitions = HiveOperator(
    task_id='user_register_add_partitions',
    hql="""
            ALTER TABLE user_register ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        """,
    schema='ofood_source',
    dag=dag)

insert_ofood_dnu = HiveOperator(
    hql="""
        insert overwrite table ofood_dnu
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


user_orders_add_partitions = HiveOperator(
    task_id='user_orders_add_partitions',
    hql="""
            ALTER TABLE user_orders ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        """,
    schema='ofood_source',
    dag=dag)

insert_ofood_order_sum = HiveOperator(
    hql="""
        insert overwrite table ofood_order_sum
        select
            dt,
            orderstatus,
            deliverytype,
            count(distinct orderid)
        FROM
            ofood_source.user_orders
        WHERE dt >= '2019-04-20'
        GROUP BY
            dt,
            orderstatus,
            deliverytype
    """,
    schema='dashboard',
    task_id='insert_ofood_order_sum',
    dag=dag)

IMPALA_QUERY = [
    "REFRESH dashboard.ofood_dau",
    "REFRESH dashboard.ofood_dau",
    "REFRESH dashboard.ofood_order_sum"
]

def impala_query(ds, **kwargs):
    conn = connect(host=Variable.get("IMPALA_URL"), port=21050)
    cur = conn.cursor()
    for sql in IMPALA_QUERY:
        cur.execute(sql)

refresh_impala = PythonOperator(
    task_id='refresh_impala',
    provide_context=True,
    python_callable=impala_query,
    dag=dag
)

user_login_add_partitions >> insert_ofood_dau
user_register_add_partitions >> insert_ofood_dnu
user_orders_add_partitions >> insert_ofood_order_sum
insert_ofood_dau >> refresh_impala
insert_ofood_dnu >> refresh_impala
insert_ofood_order_sum >> refresh_impala