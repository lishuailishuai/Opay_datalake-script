import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 4, 20),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'ofood_daily',
    schedule_interval="30 01 * * *",
    default_args=args)

create_ofood_active_user = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS ofood_active_user (
            uid int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_ofood_active_user',
    dag=dag)

insert_ofood_active_user = HiveOperator(
    hql="""
        INSERT OVERWRITE TABLE ofood_active_user PARTITION (dt='{{ ds }}')
        SELECT
            distinct uid
        FROM
            ofood_source.user_login
        WHERE dt = '{{ ds }}'
    """,
    schema='dashboard',
    task_id='insert_ofood_active_user',
    dag=dag)

create_ofood_active_user_retention = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS ofood_active_user_retention (
            install_date string,
            period int,
            retained_users int,
            cohort_size int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_ofood_active_user_retention',
    dag=dag)

insert_ofood_active_user_retention = HiveOperator(
    hql="""
        INSERT OVERWRITE TABLE ofood_active_user_retention PARTITION (dt='{{ ds }}')
        SELECT
            b.dt,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM
            (
                SELECT
                    n.dt AS dt,
                    COUNT(*) AS retained_users
                FROM
                    ofood_active_user au, ofood_active_user n
                WHERE
                    au.uid = n.uid
                    AND au.dt = '{{ ds }}'
                    AND n.dt >= '{{ macros.ds_add(ds, -30) }}'
                    AND n.dt < '{{ ds }}'
                GROUP BY
                    n.dt
            ) a
            RIGHT OUTER JOIN
            (
                SELECT
                    dt,
                    count(*) as cohort_size
                FROM
                    ofood_active_user
                WHERE
                    dt >= '{{ macros.ds_add(ds, -30) }}' AND dt < '{{ ds }}'
                GROUP BY
                    dt
            ) b ON a.dt = b.dt
    """,
    schema='dashboard',
    task_id='insert_ofood_active_user_retention',
    dag=dag)

create_ofood_register_user_retention = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS ofood_register_user_retention (
            install_date string,
            period int,
            retained_users int,
            cohort_size int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_ofood_register_user_retention',
    dag=dag)

insert_ofood_register_user_retention = HiveOperator(
    hql="""
        INSERT OVERWRITE TABLE ofood_register_user_retention PARTITION (dt='{{ ds }}')
        SELECT
            b.dt,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM
            (
                SELECT
                    n.dt AS dt,
                    COUNT(*) AS retained_users
                FROM
                    ofood_active_user au, ofood_source.user_register n
                WHERE
                    au.uid = n.uid
                    AND au.dt = '{{ ds }}'
                    AND n.dt >= '{{ macros.ds_add(ds, -30) }}'
                    AND n.dt < '{{ ds }}'
                GROUP BY
                    n.dt
            ) a
            RIGHT OUTER JOIN
            (
                SELECT
                    dt,
                    count(*) as cohort_size
                FROM
                    ofood_source.user_register
                WHERE
                    dt >= '{{ macros.ds_add(ds, -30) }}' AND dt < '{{ ds }}'
                GROUP BY
                    dt
            ) b ON a.dt = b.dt
    """,
    schema='dashboard',
    task_id='insert_ofood_register_user_retention',
    dag=dag)


create_ofood_old_user_order_sum = HiveOperator(
    hql="""
        CREATE TABLE IF NOT EXISTS ofood_old_user_order_sum (
            user_num int,
            order_num int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    task_id='create_ofood_old_user_order_sum',
    dag=dag)

insert_ofood_old_user_order_sum = HiveOperator(
    hql="""
        INSERT OVERWRITE TABLE ofood_old_user_order_sum PARTITION (dt='{{ ds }}')
        SELECT
            COUNT(distinct a.uid),
            COUNT(distinct a.orderid)
        FROM
            ofood_source.user_orders a
            INNER JOIN (
                SELECT
                    DISTINCT uid
                FROM
                    ofood_source.user_orders
                WHERE
                    dt>='{{ macros.ds_add(ds, -15) }}' and dt<'{{ ds }}' and orderstatus=8
            ) b on b.uid=a.uid
        WHERE
            a.dt='{{ ds }}' and a.orderstatus=8
    """,
    schema='dashboard',
    task_id='insert_ofood_old_user_order_sum',
    dag=dag)

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.ofood_active_user;
        REFRESH dashboard.ofood_active_user_retention;
        REFRESH dashboard.ofood_old_user_order_sum;
        REFRESH dashboard.ofood_register_user_retention;
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_ofood_active_user >> insert_ofood_active_user
create_ofood_active_user_retention >> insert_ofood_active_user_retention
create_ofood_register_user_retention >> insert_ofood_register_user_retention
create_ofood_old_user_order_sum >> insert_ofood_old_user_order_sum
insert_ofood_active_user >> insert_ofood_active_user_retention
insert_ofood_active_user >> refresh_impala
insert_ofood_active_user_retention >> refresh_impala
insert_ofood_old_user_order_sum >> refresh_impala
insert_ofood_register_user_retention >> refresh_impala