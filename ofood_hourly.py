import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

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

create_ofood_dau = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS ofood_dau (
            dt string,
            dau int
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_ofood_dau',
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

create_ofood_dnu = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS ofood_dnu (
            dt string,
            dnu int
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_ofood_dnu',
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

create_ofood_order_sum = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS ofood_order_sum (
            dt string,
            order_status int,
            delivery_type int,
            num int,
            discount int,
            amount int
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_ofood_order_sum',
    dag=dag)

insert_ofood_order_sum = HiveOperator(
    hql="""
        insert overwrite table ofood_order_sum
        select
            dt,
            orderstatus,
            deliverytype,
            count(orderid) as num,
            sum(discount) as discount,
            sum(amount) as amount
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

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.ofood_dau;
        REFRESH dashboard.ofood_dnu;
        REFRESH dashboard.ofood_order_sum;
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_ofood_dau >> insert_ofood_dau
create_ofood_dnu >> insert_ofood_dnu
create_ofood_order_sum >> insert_ofood_order_sum
user_login_add_partitions >> insert_ofood_dau
user_register_add_partitions >> insert_ofood_dnu
user_orders_add_partitions >> insert_ofood_order_sum
insert_ofood_dau >> refresh_impala
insert_ofood_dnu >> refresh_impala
insert_ofood_order_sum >> refresh_impala
