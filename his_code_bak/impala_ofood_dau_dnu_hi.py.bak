import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

args = {
    'owner': 'yangmingze',
    'start_date': datetime(2019, 9, 17),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'impala_ofood_dau_dnu_hi',
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
        -- 删除数据
        INSERT OVERWRITE TABLE ofood_dau
        SELECT
            *
        FROM
           ofood_dau
        WHERE
            dt != '{{ ds }}';
        -- 插入数据
        INSERT INTO TABLE ofood_dau
        SELECT
            dt,
            count(distinct uid)
        FROM
            ofood_source.user_login
        WHERE dt = '{{ ds }}'
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
        -- 删除数据
        INSERT OVERWRITE TABLE ofood_dnu
        SELECT
            *
        FROM
           ofood_dnu
        WHERE
            dt != '{{ ds }}';
        -- 插入数据
        INSERT INTO TABLE ofood_dnu
        SELECT
            dt,
            count(distinct uid)
        FROM
            ofood_source.user_register
        WHERE dt = '{{ ds }}'
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
        -- 删除数据
        INSERT OVERWRITE TABLE ofood_order_sum
        SELECT
            *
        FROM
           ofood_order_sum
        WHERE
            dt != '{{ ds }}';
        -- 插入数据
        INSERT INTO TABLE ofood_order_sum
        SELECT
            dt,
            orderstatus,
            deliverytype,
            count(orderid) as num,
            sum(discount) as discount,
            sum(amount) as amount
        FROM
            ofood_source.user_orders
        WHERE dt = '{{ ds }}'
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
