import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 6, 16),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_cheating_detection',
    schedule_interval="30 * * * *",
    default_args=args)

add_partitions = HiveOperator(
    task_id='add_partitions',
    hql="""
            ALTER TABLE server_event ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        """,
    schema='oride_source',
    dag=dag)

create_oride_cheating_detection_detail = HiveOperator(
    task_id='create_oride_cheating_detection_detail',
    hql="""
        CREATE TABLE IF NOT EXISTS `cheating_detection_detail`(
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
            `event_value` string --{"bind_time":"1560639705","bind_device_id":"97B22A1F2BFDC3F3976C3734244AE12A","bind_ip":"105.112.11.30","bind_net_type":"","bind_number":"+2348144397432","bind_refferal_code":"159274"}
        )
        PARTITIONED BY (
            `dt` string,
            `hour` string)
        STORED AS PARQUET
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_cheating_detection_detail = HiveOperator(
    task_id='insert_oride_cheating_detection_detail',
    hql="""
        -- 删除数据
        ALTER TABLE cheating_detection_detail DROP IF EXISTS PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}');
        INSERT OVERWRITE TABLE cheating_detection_detail PARTITION (dt = '{{ ds }}', hour = '{{ execution_date.strftime("%H") }}')
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
            AND e.event_name='invitation_code_click_confirm'
        """,
    schema='oride_bi',
    dag=dag)

clear_promoter_mysql_data = MySqlOperator(
    task_id='clear_promoter_mysql_data',
    sql="""
        DELETE FROM promoter_users_device WHERE dt='{{ ds_nodash }}' AND hour='{{ execution_date.hour }}';
        DELETE FROM promoter_data_day WHERE day='{{ ds_nodash }}';
        DELETE FROM promoter_data_hour WHERE day='{{ ds_nodash }}' and hour='{{ execution_date.hour}}';
    """,
    mysql_conn_id='opay_spread_mysql',
    dag=dag)

promoter_detail_to_msyql = HiveToMySqlTransfer(
    task_id='promoter_detail_to_msyql',
    sql="""
            SELECT
                null as id,
                get_json_object(event_value, '$.bind_refferal_code') as code,
                get_json_object(event_value, '$.bind_number') as phone,
                get_json_object(event_value, '$.bind_device_id') as device_id,
                0 as register_time,
                get_json_object(event_value, '$.bind_time') as bind_time,
                {{ ds_nodash }},
                {{ execution_date.hour}},
                ip
            FROM
                oride_bi.cheating_detection_detail
            WHERE
                  dt='{{ ds }}'
                  AND hour='{{ execution_date.strftime("%H") }}'
        """,
    mysql_conn_id='opay_spread_mysql',
    mysql_table='promoter_users_device',
    dag=dag)

promoter_hour_to_msyql = HiveToMySqlTransfer(
    task_id='promoter_hour_to_msyql',
    sql="""
            SELECT
                null as id,
                get_json_object(event_value, '$.bind_refferal_code') as code,
                from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'), 'yyyyMMdd'),
                cast(hour as int),
                COUNT(DISTINCT get_json_object(event_value, '$.bind_number')) as users_count,
                COUNT(DISTINCT if (length(get_json_object(event_value, '$.bind_device_id'))>0, get_json_object(event_value, '$.bind_device_id'), NULL)) as device_count,
                unix_timestamp()
            FROM
                oride_bi.cheating_detection_detail
            WHERE
                dt='{{ ds }}'
                AND hour='{{ execution_date.strftime("%H") }}'
            GROUP BY
                get_json_object(event_value, '$.bind_refferal_code'),
                dt,
                hour
        """,
    mysql_conn_id='opay_spread_mysql',
    mysql_table='promoter_data_hour',
    dag=dag)

promoter_day_to_msyql = HiveToMySqlTransfer(
    task_id='promoter_day_to_msyql',
    # sql="""
    #        SELECT
    #            null as id,
    #            get_json_object(event_value, '$.bind_refferal_code') as code,
    #            from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'), 'yyyyMMdd'),
    #            COUNT(DISTINCT get_json_object(event_value, '$.bind_number')) as users_count,
    #            COUNT(DISTINCT if (length(get_json_object(event_value, '$.bind_device_id'))>0, get_json_object(event_value, '$.bind_device_id'), NULL)) as device_count,
    #            unix_timestamp()
    #        FROM
    #            oride_bi.cheating_detection_detail
    #        WHERE
    #            dt='{{ ds }}'
    #        GROUP BY
    #            get_json_object(event_value, '$.bind_refferal_code'),
    #            dt
    #    """,
    sql="""
        SELECT 
            NULL as id,
            NVL(a.code, b.code),
            NVL(a.day, b.day),
            NVL(a.users_count, 0),
            NVL(a.device_count, 0),
            unix_timestamp(), 
            NVL(b.orders_f, 0) 
        FROM (SELECT
                get_json_object(event_value, '$.bind_refferal_code') as code,
                from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'), 'yyyyMMdd') as day,
                COUNT(DISTINCT get_json_object(event_value, '$.bind_number')) as users_count,
                COUNT(DISTINCT if (length(get_json_object(event_value, '$.bind_device_id'))>0, get_json_object(event_value, '$.bind_device_id'), NULL)) as device_count
            FROM oride_bi.cheating_detection_detail
            WHERE dt = '{{ ds }}'
            GROUP BY get_json_object(event_value, '$.bind_refferal_code'), dt
            ) AS a 
        FULL OUTER JOIN (SELECT
                c.code,
                from_unixtime(unix_timestamp(o.dt,'yyyy-MM-dd'), 'yyyyMMdd') AS day,
                COUNT(DISTINCT o.orders_f) as orders_f 
            FROM (SELECT 
                    user_id,
                    get_json_object(event_value, '$.bind_refferal_code') AS code 
                FROM oride_bi.cheating_detection_detail 
                ) AS c 
            JOIN (SELECT 
                    dt,
                    user_id,
                    id as orders_f 
                FROM oride_dw_ods.ods_binlog_data_order_hi 
                WHERE dt = '{{ ds }}' AND 
                    status IN (4,5) AND 
                    from_unixtime(arrive_time, 'yyyy-MM-dd') = '{{ ds }}' 
                ) AS o 
            ON c.user_id = o.user_id 
            GROUP BY c.code, o.dt 
            ) AS b 
        ON a.code = b.code AND a.day = b.day 
    """,
    mysql_conn_id='opay_spread_mysql',
    mysql_table='promoter_data_day',
    dag=dag)

add_partitions >> insert_oride_cheating_detection_detail
create_oride_cheating_detection_detail >> insert_oride_cheating_detection_detail
insert_oride_cheating_detection_detail >> promoter_detail_to_msyql
insert_oride_cheating_detection_detail >> promoter_day_to_msyql
insert_oride_cheating_detection_detail >> promoter_hour_to_msyql
clear_promoter_mysql_data >> promoter_detail_to_msyql
clear_promoter_mysql_data >> promoter_day_to_msyql
clear_promoter_mysql_data >> promoter_hour_to_msyql
