import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 28),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = airflow.DAG(
    'oride_retain',
    schedule_interval="30 01 * * *",
    default_args=args)

create_oride_active_user = HiveOperator(
    task_id='create_oride_active_user',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_active_user(
          user_id bigint,
          phone_number string,
          is_new boolean,
          `timestamp` bigint,
          appsflyer_id string
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='dashboard',
    dag=dag)

insert_oride_active_user = HiveOperator(
    task_id='insert_oride_active_user',
    hql="""
        INSERT OVERWRITE TABLE oride_active_user PARTITION (dt='{{ ds }}')
        SELECT
            user_id,
            MAX(struct(`timestamp`, phone_number)).col2 AS phone_number,
            MAX(is_new) AS is_new,
            MAX(`timestamp`) AS `timestamp`,
            MAX(struct(`timestamp`, appsflyer_id)).col2 AS appsflyer_id
        FROM
            oride_source.user_login
        WHERE
            dt='{{ ds }}'
        GROUP BY
            user_id
    """,
    schema='dashboard',
    dag=dag)

create_oride_active_user_retain = HiveOperator(
    task_id='create_oride_active_user_retain',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_active_user_retain (
          `date` string,
          period int,
          retained_users int,
          cohort_size INT
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)

insert_oride_active_user_retain = HiveOperator(
    task_id='insert_oride_active_user_retain',
    hql="""
        ALTER TABLE oride_active_user_retain DROP IF EXISTS PARTITION (dt='{{ ds }}');
        INSERT OVERWRITE TABLE oride_active_user_retain PARTITION (dt='{{ ds }}')
        SELECT
            b.dt,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM (
            SELECT
                n.dt AS dt,
                COUNT(*) AS retained_users
            FROM oride_active_user au, oride_active_user n
            WHERE au.user_id = n.user_id
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
            FROM oride_active_user
            WHERE dt >= '{{ macros.ds_add(ds, -30) }}' AND dt < '{{ ds }}'
            GROUP BY
                dt
        ) b
        ON
            a.dt = b.dt
    """,
    schema='dashboard',
    dag=dag)

create_oride_new_user_retain = HiveOperator(
    task_id='create_oride_new_user_retain',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_new_user_retain (
          `date` string,
          period int,
          retained_users int,
          cohort_size INT
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='dashboard',
    dag=dag)


insert_oride_new_user_retain = HiveOperator(
    task_id='insert_oride_new_user_retain',
    hql="""
        ALTER TABLE oride_new_user_retain DROP IF EXISTS PARTITION (dt='{{ ds }}');
        INSERT OVERWRITE TABLE oride_new_user_retain PARTITION (dt='{{ ds }}')
        SELECT
            b.dt,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM (
            SELECT
                n.dt AS dt,
                COUNT(*) AS retained_users
            FROM oride_active_user au, oride_active_user n
            WHERE au.user_id = n.user_id
                AND au.dt = '{{ ds }}'
                AND n.dt >= '{{ macros.ds_add(ds, -30) }}'
                AND n.dt < '{{ ds }}'
                AND n.is_new=true
            GROUP BY
                n.dt
        ) a
        RIGHT OUTER JOIN
        (
            SELECT
                dt,
                count(*) as cohort_size
            FROM oride_active_user
            WHERE dt >= '{{ macros.ds_add(ds, -30) }}' AND dt < '{{ ds }}' AND is_new=true
            GROUP BY
                dt
        ) b
        ON
            a.dt = b.dt
    """,
    schema='dashboard',
    dag=dag)

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.oride_active_user PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_active_user_retain PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_new_user_retain PARTITION (dt='{{ds}}');
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_active_user >> insert_oride_active_user >> refresh_impala
create_oride_active_user_retain >> insert_oride_active_user_retain >> refresh_impala
create_oride_new_user_retain >> insert_oride_new_user_retain >> refresh_impala
insert_oride_active_user >> insert_oride_active_user_retain
insert_oride_active_user >> insert_oride_new_user_retain
