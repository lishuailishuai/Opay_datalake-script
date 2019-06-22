import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 27),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_retain',
    schedule_interval="50 01 * * *",
    default_args=args)

create_oride_new_user_channel = HiveOperator(
    task_id='create_oride_new_user_channel',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_new_user_channel(
          user_id bigint,
          channel string
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET

    """,
    schema='dashboard',
    dag=dag)

insert_oride_new_user_channel = HiveOperator(
    task_id='insert_oride_new_user_channel',
    hql="""
        ALTER TABLE oride_new_user_channel DROP IF EXISTS PARTITION (dt='{{ ds }}');
        INSERT OVERWRITE TABLE oride_new_user_channel PARTITION (dt='{{ ds }}')
        SELECT
            ul.user_id,
            case
                when due.inviter_id>0 then 'invite'
                when length(du.promoter_code) !=0 then 'offline'
                when isnotnull(aoi.media_source) then 'online'
                else 'organic'
            end as channel
        FROM
            oride_source.user_login ul
            INNER JOIN oride_db.data_user du on du.dt=ul.dt AND du.id=ul.user_id
            INNER JOIN oride_db.data_user_extend due on due.dt=ul.dt AND due.id=ul.user_id
            LEFT JOIN oride_source.appsflyer_opay_install_log aoi on aoi.dt=ul.dt AND aoi.appsflyer_id = ul.appsflyer_id
        WHERE
            ul.is_new=true and ul.dt='{{ ds }}'
    """,
    schema='dashboard',
    dag=dag)

create_oride_active_user = HiveOperator(
    task_id='create_oride_active_user',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_active_user(
          user_id bigint,
          phone_number string,
          is_new boolean,
          `timestamp` bigint,
          appsflyer_id string,
          is_request_order boolean,
          channel string
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
            ul.user_id,
            ul.phone_number,
            ul.is_new,
            ul.`timestamp`,
            ul.appsflyer_id,
            if (nvl(uo.user_id, 0) =0, false, true) as is_request_order,
            nvl(onu.channel, 'organic') as channel
        FROM
            (
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
            ) ul
            LEFT JOIN (
                SELECT
                    distinct user_id as user_id
                FROM
                    oride_db.data_order
                WHERE
                    dt='{{ ds }}' AND  from_unixtime(create_time, 'yyyy-MM-dd') = dt
            ) uo ON uo.user_id = ul.user_id
            LEFT JOIN
                dashboard.oride_new_user_channel onu ON onu.user_id=ul.user_id
    """,
    schema='dashboard',
    dag=dag)

create_oride_active_user_retain = HiveOperator(
    task_id='create_oride_active_user_retain',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_active_user_retain (
          `date` string,
          channel string,
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
            b.channel,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM (
            SELECT
                n.dt AS dt,
                n.channel as channel,
                COUNT(*) AS retained_users
            FROM oride_active_user au, oride_active_user n
            WHERE au.user_id = n.user_id
                AND au.dt = '{{ ds }}'
                AND n.dt >= '{{ macros.ds_add(ds, -30) }}'
                AND n.dt < '{{ ds }}'
            GROUP BY
                n.dt,
                n.channel
        ) a
        RIGHT OUTER JOIN
        (
            SELECT
                dt,
                channel,
                count(*) as cohort_size
            FROM oride_active_user
            WHERE dt >= '{{ macros.ds_add(ds, -30) }}' AND dt < '{{ ds }}'
            GROUP BY
                dt,channel
        ) b
        ON
            a.dt = b.dt
            AND a.channel=b.channel
    """,
    schema='dashboard',
    dag=dag)

create_oride_new_user_retain = HiveOperator(
    task_id='create_oride_new_user_retain',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_new_user_retain (
          `date` string,
          channel string,
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
            b.channel,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM (
            SELECT
                n.dt AS dt,
                n.channel as channel,
                COUNT(*) AS retained_users
            FROM oride_active_user au, oride_active_user n
            WHERE au.user_id = n.user_id
                AND au.dt = '{{ ds }}'
                AND n.dt >= '{{ macros.ds_add(ds, -30) }}'
                AND n.dt < '{{ ds }}'
                AND n.is_new=true
            GROUP BY
                n.dt,n.channel
        ) a
        RIGHT OUTER JOIN
        (
            SELECT
                dt,
                channel,
                count(*) as cohort_size
            FROM oride_active_user
            WHERE dt >= '{{ macros.ds_add(ds, -30) }}' AND dt < '{{ ds }}' AND is_new=true
            GROUP BY
                dt,channel
        ) b
        ON
            a.dt = b.dt AND a.channel=b.channel
    """,
    schema='dashboard',
    dag=dag)

create_oride_order_user_retain = HiveOperator(
    task_id='create_oride_order_user_retain',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_order_user_retain (
          `date` string,
          channel string,
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


insert_oride_order_user_retain = HiveOperator(
    task_id='insert_oride_order_user_retain',
    hql="""
        ALTER TABLE oride_order_user_retain DROP IF EXISTS PARTITION (dt='{{ ds }}');
        INSERT OVERWRITE TABLE oride_order_user_retain PARTITION (dt='{{ ds }}')
        SELECT
            b.dt,
            b.channel,
            DATEDIFF('{{ ds }}', b.dt) AS period,
            NVL(a.retained_users, 0) AS retained_users,
            b.cohort_size
        FROM (
            SELECT
                n.dt AS dt,
                n.channel AS channel,
                COUNT(*) AS retained_users
            FROM oride_active_user au, oride_active_user n
            WHERE au.user_id = n.user_id
                AND au.dt = '{{ ds }}'
                AND n.dt >= '{{ macros.ds_add(ds, -30) }}'
                AND n.dt <= '{{ ds }}'
                AND n.is_new=true
                AND n.is_request_order=true
            GROUP BY
                n.dt,n.channel
        ) a
        RIGHT OUTER JOIN
        (
            SELECT
                dt,
                channel,
                count(*) as cohort_size
            FROM oride_active_user
            WHERE dt >= '{{ macros.ds_add(ds, -30) }}' AND dt <= '{{ ds }}' AND is_new=true
            GROUP BY
                dt,channel
        ) b
        ON
            a.dt = b.dt AND a.channel=b.channel
    """,
    schema='dashboard',
    dag=dag)

refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH dashboard.oride_new_user_channel PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_active_user PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_active_user_retain PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_new_user_retain PARTITION (dt='{{ds}}');
        REFRESH dashboard.oride_order_user_retain PARTITION (dt='{{ds}}');
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_new_user_channel >> insert_oride_new_user_channel >> refresh_impala
insert_oride_new_user_channel >> insert_oride_active_user
create_oride_active_user >> insert_oride_active_user >> refresh_impala
create_oride_active_user_retain >> insert_oride_active_user_retain >> refresh_impala
create_oride_new_user_retain >> insert_oride_new_user_retain >> refresh_impala
create_oride_order_user_retain >> insert_oride_order_user_retain >> refresh_impala
insert_oride_active_user >> insert_oride_active_user_retain
insert_oride_active_user >> insert_oride_new_user_retain
insert_oride_active_user >> insert_oride_order_user_retain

