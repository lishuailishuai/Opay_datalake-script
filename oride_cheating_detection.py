import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.hive_to_mysql import HiveToMySqlTransfer
from airflow.operators.mysql_operator import MySqlOperator
from airflow.sensors import UFileSensor

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




# 依赖前一天分区
dwd_oride_driver_cheating_detection_hi_prev_hour_task = UFileSensor(
    task_id='dwd_oride_driver_cheating_detection_hi_prev_hour_task',
    filepath='{hdfs_path_str}/dt={pt}/hour={hour}/_SUCCESS'.format(
        hdfs_path_str="oride/oride_dw/dwd_oride_driver_cheating_detection_hi/country_code=nal",
        pt='{{ds}}',
        hour='{{ execution_date.strftime("%H") }}'
    ),
    bucket_name='opay-datalake',
    poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
    dag=dag
)



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
                oride_dw.dwd_oride_driver_cheating_detection_hi
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
                oride_dw.dwd_oride_driver_cheating_detection_hi
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
    #            oride_dw.dwd_oride_driver_cheating_detection_hi
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
            NVL(b.orders_f, 0) ,
            0,
            0
        FROM (SELECT
                get_json_object(event_value, '$.bind_refferal_code') as code,
                from_unixtime(unix_timestamp(dt,'yyyy-MM-dd'), 'yyyyMMdd') as day,
                COUNT(DISTINCT get_json_object(event_value, '$.bind_number')) as users_count,
                COUNT(DISTINCT if (length(get_json_object(event_value, '$.bind_device_id'))>0, get_json_object(event_value, '$.bind_device_id'), NULL)) as device_count
            FROM oride_dw.dwd_oride_driver_cheating_detection_hi
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
                FROM oride_dw.dwd_oride_driver_cheating_detection_hi 
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

dwd_oride_driver_cheating_detection_hi_prev_hour_task >> promoter_detail_to_msyql
dwd_oride_driver_cheating_detection_hi_prev_hour_task >> promoter_day_to_msyql
dwd_oride_driver_cheating_detection_hi_prev_hour_task >> promoter_hour_to_msyql
clear_promoter_mysql_data >> promoter_detail_to_msyql
clear_promoter_mysql_data >> promoter_day_to_msyql
clear_promoter_mysql_data >> promoter_hour_to_msyql
