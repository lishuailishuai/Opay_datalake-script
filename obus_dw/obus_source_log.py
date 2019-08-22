import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 8, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'obus_source_log',
    schedule_interval="10 * * * *",
    default_args=args)

create_obus_client_event = HiveOperator(
    task_id='create_obus_client_event',
    hql="""
        CREATE TABLE IF NOT EXISTS `ods_log_client_event`(
            `ip` string,
            `server_ip` string,
            `timestamp` bigint,
            `common` struct<
                `user_id`:string,
                `user_number`:string,
                `client_timestamp`:string,
                `platform`:string,
                `os_version`:string,
                `app_name`:string,
                `app_version`:string,
                `locale`:string,
                `device_id`:string,
                `device_screen`:string,
                `device_model`:string,
                `device_manufacturer`:string,
                `is_root`:string,
                `channel`:string,
                `subchannel`:string,
                `gaid`:string,
                `appsflyer_id`:string
            >,
            `events` Array<
                struct<
                    `event_time`:string,
                    `event_name`:string,
                    `page`:string,
                    `source`:string,
                    `event_value`:string
                >
            >
        )
        PARTITIONED BY (
            `dt` string,
            `hour` string)
        ROW FORMAT SERDE
            'org.openx.data.jsonserde.JsonSerDe'
        with SERDEPROPERTIES("ignore.malformed.json"="true")
        LOCATION 's3a://opay-bi/obus_buried/obdm.client_event'
    """,
    schema='obus_dw',
    dag=dag)

check_s3_obus_client_event = S3PrefixSensor(
    task_id='check_s3_obus_client_event',
    prefix='obus_buried/obdm.client_event/dt={{ ds }}/hour={{ execution_date.strftime("%H") }}',
    bucket_name='opay-bi',
    dag=dag)

add_partitions_obus_client_event = HiveOperator(
    task_id='add_partitions_obus_client_event',
    hql="""
            ALTER TABLE ods_log_client_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='obus_dw',
    dag=dag)

create_obus_server_event = HiveOperator(
    task_id='create_obus_server_event',
    hql="""
        CREATE TABLE IF NOT EXISTS `ods_log_server_event`(
            `ip` string,
            `server_ip` string,
            `timestamp` bigint,
            `common` struct<
                `user_id`:string,
                `user_number`:string,
                `client_timestamp`:string,
                `platform`:string,
                `os_version`:string,
                `app_name`:string,
                `app_version`:string,
                `locale`:string,
                `device_id`:string,
                `device_screen`:string,
                `device_model`:string,
                `device_manufacturer`:string,
                `is_root`:string,
                `channel`:string,
                `subchannel`:string,
                `gaid`:string,
                `appsflyer_id`:string
            >,
            `events` Array<
                struct<
                    `event_time`:string,
                    `event_name`:string,
                    `page`:string,
                    `source`:string,
                    `event_value`:string
                >
            >
        )
        PARTITIONED BY (
            `dt` string,
            `hour` string)
        ROW FORMAT SERDE
            'org.openx.data.jsonserde.JsonSerDe'
        with SERDEPROPERTIES("ignore.malformed.json"="true")
        LOCATION 's3a://opay-bi/obus_buried/obdm.server_event'
    """,
    schema='obus_dw',
    dag=dag)

check_s3_obus_server_event = S3PrefixSensor(
    task_id='check_s3_obus_server_event',
    prefix='obus_buried/obdm.server_event/dt={{ ds }}/hour={{ execution_date.strftime("%H") }}',
    bucket_name='opay-bi',
    dag=dag)

add_partitions_obus_server_event = HiveOperator(
    task_id='add_partitions_obus_server_event',
    hql="""
            ALTER TABLE ods_log_server_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='obus_dw',
    dag=dag)

create_obus_client_event >> check_s3_obus_client_event >> add_partitions_obus_client_event
create_obus_server_event >> check_s3_obus_server_event >> add_partitions_obus_server_event
