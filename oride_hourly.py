import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

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

user_login_add_partitions = HiveOperator(
    task_id='user_login_add_partitions',
    hql="""
            ALTER TABLE user_login ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        """,
    schema='oride_source',
    dag=dag)

create_oride_dau = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS oride_dau (
            dt string,
            dau int
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_oride_dau',
    dag=dag)

insert_oride_dau = HiveOperator(
    hql="""
        insert overwrite table oride_dau
        select
            dt,
            count(distinct user_id)
        FROM
            oride_source.user_login
        WHERE dt >= '2019-05-11'
        GROUP BY
            dt
    """,
    schema='dashboard',
    task_id='insert_oride_dau',
    dag=dag)

create_oride_dnu = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS oride_dnu (
            dt string,
            dnu int
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_oride_dnu',
    dag=dag)

insert_oride_dnu = HiveOperator(
    hql="""
        insert overwrite table oride_dnu
        select
            dt,
            count(distinct user_id)
        FROM
            oride_source.user_login
        WHERE dt >= '2019-04-20' and is_new=true
        GROUP BY
            dt
    """,
    schema='dashboard',
    task_id='insert_oride_dnu',
    dag=dag)


user_orders_add_partitions = HiveOperator(
    task_id='user_orders_add_partitions',
    hql="""
            ALTER TABLE user_orders ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        """,
    schema='oride_source',
    dag=dag)

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.oride_dau;
        REFRESH dashboard.oride_dnu;
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_dau >> insert_oride_dau
create_oride_dnu >> insert_oride_dnu
user_login_add_partitions >> insert_oride_dau
user_login_add_partitions  >> insert_oride_dnu
insert_oride_dau >> refresh_impala
insert_oride_dnu >> refresh_impala
