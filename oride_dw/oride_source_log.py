import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator

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
            ALTER TABLE ods_log_oride_trip_raw_feature_hi ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='oride_dw_ods',
    dag=dag)

add_algo_partitions = HiveOperator(
    task_id='add_algo_partitions',
    hql="""
            ALTER TABLE oride_algo_score_data ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE oride_algo_trip_score_data ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE can_carpool_order ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE oride_trip_stat_feature ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='algo',
    dag=dag)

add_partitions = HiveOperator(
    task_id='add_partitions',
    hql="""
            ALTER TABLE moto_locations ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE client_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE opay_ep_logv0 ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE opay_ep_logv1 ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE server_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE server_magic ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE h5_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE log_anti_ofood_oride_fraud ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE log_anti_oride_fraud ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE dispatch_tracker_server_magic ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_accept_order_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_arrive_order_dest_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_arrive_order_start_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_assign_sheet_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_assign_sheet_timeout_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_cancel_order_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_create_order_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_estimate_order_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_finish_order_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_refuse_order_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_send_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE algo_send_order_hook ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE uops_user_user_tag ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE uops_user_driver_tag ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE order_driver_feature_new ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE hex_supply_demand_feature ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
            ALTER TABLE opay_ad_feature ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
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
        LOCATION 'oss://opay-datalake/oride/client_event_detail'
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
        LOCATION 'oss://opay-datalake/oride/server_event_detail'
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


add_partitions >> insert_oride_client_event_detail
add_partitions >> insert_oride_server_event_detail
create_oride_client_event_detail >> insert_oride_client_event_detail
create_oride_server_event_detail >> insert_oride_server_event_detail
