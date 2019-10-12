import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.impala_plugin import ImpalaOperator

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 5, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_source_log',
    schedule_interval="10 * * * *",
    default_args=args)


add_dw_partitions = HiveOperator(
    task_id='add_dw_partitions',
    hql="""
            ALTER TABLE ods_log_user_track_data_hi ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE ods_log_driver_track_data_hi ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            
        """,
    schema='oride_dw_ods',
    dag=dag)

add_partitions = HiveOperator(
    task_id='add_partitions',
    hql="""
            ALTER TABLE driver_action ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE moto_locations ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE order_locations ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_action ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_login ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_order ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE user_payment ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE client_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE server_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE server_magic ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE anti_fraud ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE h5_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE log_anti_ofood_oride_fraud ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE dispatch_tracker_server_magic ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_accept_order_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_arrive_order_dest_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_arrive_order_start_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_assign_sheet_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_assign_sheet_timeout_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_cancel_order_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_create_order_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_estimate_order_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_finish_order_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_refuse_order_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_send_hookADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_send_order_hook IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='oride_source',
    dag=dag)

create_oride_client_event_detail = HiveOperator(
    task_id='create_oride_client_event_detail',
    hql="""
        CREATE TABLE IF NOT EXISTS `oride_client_event_detail`(
            `ip` string,
            `server_ip` string,
            `timestamp` bigint,
            `user_id` string,
            `user_number` string,
            `client_timestamp` string,
            `platform` string,
            `os_version` string,
            `app_name` string,
            `app_version` string,
            `locale` string,
            `device_id` string,
            `device_screen` string,
            `device_model` string,
            `device_manufacturer` string,
            `is_root` string,
            `channel` string,
            `subchannel` string,
            `gaid` string,
            `appsflyer_id` string,
            `event_time` string,
            `event_name` string,
            `page` string,
            `source` string,
            `event_value` string
        )
        PARTITIONED BY (
            `dt` string,
            `hour` string)
        STORED AS ORC
        LOCATION 's3a://opay-bi/oride_dw_ods/oride_client_event_detail'
        TBLPROPERTIES ("orc.compress"="SNAPPY")
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_client_event_detail = HiveOperator(
    task_id='insert_oride_client_event_detail',
    hql="""
        -- 删除数据
        ALTER TABLE oride_client_event_detail DROP IF EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        INSERT OVERWRITE TABLE oride_client_event_detail PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        SELECT
            ip,
            server_ip,
            `timestamp`,
            common.user_id,
            common.user_number,
            common.client_timestamp,
            common.platform,
            common.os_version,
            common.app_name,
            common.app_version,
            common.locale,
            common.device_id,
            common.device_screen,
            common.device_model,
            common.device_manufacturer,
            common.is_root,
            common.channel,
            common.subchannel,
            common.gaid,
            common.appsflyer_id,
            e.event_time,
            e.event_name,
            e.page,
            e.source,
            e.event_value
        FROM
            oride_source.client_event LATERAL VIEW EXPLODE(events) es AS e
        WHERE
            dt='{{ ds }}'
            AND hour='{{ execution_date.strftime("%H") }}'
        """,
    schema='oride_bi',
    dag=dag)

create_oride_server_event_detail = HiveOperator(
    task_id='create_oride_server_event_detail',
    hql="""
        CREATE TABLE IF NOT EXISTS `oride_server_event_detail`(
            `ip` string,
            `server_ip` string,
            `timestamp` bigint,
            `user_id` string,
            `user_number` string,
            `client_timestamp` string,
            `platform` string,
            `os_version` string,
            `app_name` string,
            `app_version` string,
            `locale` string,
            `device_id` string,
            `device_screen` string,
            `device_model` string,
            `device_manufacturer` string,
            `is_root` string,
            `channel` string,
            `subchannel` string,
            `gaid` string,
            `appsflyer_id` string,
            `event_time` string,
            `event_name` string,
            `page` string,
            `source` string,
            `event_value` string
        )
        PARTITIONED BY (
            `dt` string,
            `hour` string)
        STORED AS ORC
        LOCATION 's3a://opay-bi/oride_dw_ods/oride_server_event_detail'
        TBLPROPERTIES ("orc.compress"="SNAPPY")
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_server_event_detail = HiveOperator(
    task_id='insert_oride_server_event_detail',
    hql="""
        -- 删除数据
        ALTER TABLE oride_server_event_detail DROP IF EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        INSERT OVERWRITE TABLE oride_server_event_detail PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
        SELECT
            ip,
            server_ip,
            `timestamp`,
            common.user_id,
            common.user_number,
            common.client_timestamp,
            common.platform,
            common.os_version,
            common.app_name,
            common.app_version,
            common.locale,
            common.device_id,
            common.device_screen,
            common.device_model,
            common.device_manufacturer,
            common.is_root,
            common.channel,
            common.subchannel,
            common.gaid,
            common.appsflyer_id,
            e.event_time,
            e.event_name,
            e.page,
            e.source,
            e.event_value
        FROM
            oride_source.server_event LATERAL VIEW EXPLODE(events) es AS e
        WHERE
            dt='{{ ds }}'
            AND hour='{{ execution_date.strftime("%H") }}'
        """,
    schema='oride_bi',
    dag=dag)

create_oride_realtime_overview = HiveOperator(
    task_id='create_oride_realtime_overview',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_realtime_overview (
          dt string,
          up_hour string,
          dau int,
          dnu int,
          order_amount int,
          order_num int,
          canceled_order_num int,
          completed_order_num int,
          new_user_completed_order_num int
        )
        STORED AS PARQUET
        """,
    schema='dashboard',
    dag=dag)

insert_oride_realtime_overview = HiveOperator(
    task_id='insert_oride_realtime_overview',
    hql="""
        -- 删除数据
        INSERT OVERWRITE TABLE oride_realtime_overview
        SELECT
            *
        FROM
           oride_realtime_overview
        WHERE
            dt != '{{ ds }}';
        -- 插入数据
        with user_data as (
            select
                dt,
                count(distinct user_id) as dau,
                count(distinct if(is_new=true, user_id, null)) as dnu
            from
                oride_source.user_login
            where
                dt='{{ ds }}'
            group by
                dt
        ),
        order_data as (
            select
                t.dt as dt,
                sum(t.price) as order_amount,
                count(t.order_id) as order_num,
                sum(if(t.status=5, 1, 0)) as completed_order_num,
                sum(if(t.status>=6 and t.status<=12, 1, 0)) as canceled_order_num,
                sum(if(t.status=5 and t.is_new_order=true, 1, 0)) as new_user_completed_order_num
            from
            (
                select
                    o.dt as dt,
                    o.order_id as order_id,
                    o.status as status,
                    o.price as price,
                    if(isnotnull(nu.user_id), true, false) as is_new_order
                from
                (
                    select
                        dt,
                        order_id,
                        MAX(struct(`timestamp`, status)).col2 AS status,
                        MAX(struct(`timestamp`, price)).col2 AS price,
                        MAX(struct(`timestamp`, user_id)).col2 AS user_id
                    from
                        oride_source.user_order
                    where
                        dt='{{ ds }}'
                    group by
                        order_id,dt
                ) o
                INNER JOIN (
                    SELECT
                        distinct order_id
                    FROM
                        oride_source.user_order
                    WHERE
                        dt='{{ ds }}' AND status=0 AND from_unixtime(`timestamp`, 'yyyy-MM-dd')=dt
                ) oc ON oc.order_id=o.order_id
                LEFT JOIN
                (
                    select
                        distinct user_id
                    from
                        oride_source.user_login
                    where
                        dt = '{{ ds }}' and is_new=true
                ) nu on nu.user_id=o.user_id
            ) t
            group by t.dt
        )
        INSERT INTO TABLE oride_realtime_overview
        SELECT
            ud.dt,
            '{{ execution_date.strftime("%H") }}',
            ud.dau,
            ud.dnu,
            nvl(od.order_amount,0),
            nvl(od.order_num, 0),
            nvl(od.canceled_order_num, 0),
            nvl(od.completed_order_num, 0),
            nvl(new_user_completed_order_num, 0)
        FROM
            user_data ud
            LEFT JOIN order_data od ON od.dt=ud.dt
        """,
    schema='dashboard',
    dag=dag)


refresh_impala = ImpalaOperator(
    task_id = 'refresh_impala',
    hql="""\
        REFRESH oride_realtime_overview;
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_oride_realtime_overview >> insert_oride_realtime_overview >> refresh_impala
add_partitions >> insert_oride_realtime_overview
add_partitions >> insert_oride_client_event_detail
add_partitions >> insert_oride_server_event_detail
create_oride_client_event_detail >> insert_oride_client_event_detail
create_oride_server_event_detail >> insert_oride_server_event_detail
