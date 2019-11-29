# coding=utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.hive_operator import HiveOperator
from utils.connection_helper import get_hive_cursor, get_db_conn, get_pika_connection, get_redis_connection
from airflow.hooks.hive_hooks import HiveCliHook
from airflow import macros
import logging
from airflow.models import Variable
import pandas as pd
import io
import requests
import os
from airflow.sensors.hive_partition_sensor import HivePartitionSensor
from utils.validate_metrics_utils import *

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 7, 16),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_global_daily_report',
    schedule_interval="30 01 * * *",
    default_args=args)

global_table_names = [
    'oride_dw_ods.ods_sqoop_base_data_order_df',
    'oride_dw_ods.ods_sqoop_base_data_order_payment_df',
    'oride_dw_ods.ods_sqoop_base_data_user_extend_df',
    'oride_dw_ods.ods_sqoop_base_data_driver_extend_df',
    'oride_dw_ods.ods_log_oride_driver_timerange',
    'oride_dw.dwd_oride_order_push_driver_detail_di',
    'oride_source.server_magic',
    'oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df',
    'oride_dw_ods.ods_sqoop_base_data_driver_reward_df',
    'oride_dw_ods.ods_sqoop_base_data_city_conf_df',
]

funnel_report_table_names = [
    'oride_dw_ods.ods_sqoop_base_weather_per_10min_df',
    'oride_dw_ods.ods_sqoop_base_data_order_df',
    'oride_dw_ods.ods_sqoop_base_data_city_conf_df',
    'oride_dw_ods.ods_sqoop_base_data_order_payment_df',
    'oride_dw.dwd_oride_order_push_driver_detail_di',
]

anti_fraud_table_names = [
    'oride_dw_ods.ods_sqoop_base_data_anti_fraud_strategy_df',
    'oride_source.anti_fraud',
    'oride_dw_ods.ods_sqoop_base_data_abnormal_order_df',
]

def import_opay_event(ds, **kwargs):
    # download report
    api_url = "https://hq.appsflyer.com/export/team.opay.pay/partners_by_date_report/v5?api_token={api_token}&from={dt}&to={dt}".format(
        api_token=Variable.get("opay_appsflyer_api_token"), dt=ds)
    headers = {'Accept': 'text/csv'}
    response = requests.get(
        api_url,
        headers=headers
    )
    logging.info('url:{} response_len:{}'.format(response.url, len(response.content)))
    cols = [
        "Agency/PMD (af_prt)",
        "Media Source (pid)",
        "Campaign (c)",
        "Impressions",
        "Clicks",
        "CTR",
        "Installs",
        "Conversion Rate",
        "Sessions",
        "Loyal Users",
        "Loyal Users/Installs",
        "Total Revenue",
        "Total Cost",
        "ROI",
        "ARPU",
        "Average eCPI",
        "estimated_price_cllick_request (Event counter)",
        "oride_cllick_request (Event counter)",
    ]
    df = pd.read_csv(io.BytesIO(response.content), usecols=cols)[cols]
    tmp_path = '/data/airflow/tmp/'
    file_name = 'appsflyer_opay_event_log_' + ds
    tmp_file = tmp_path + file_name
    df.to_csv(tmp_file, index=None, header=True)
    # upload to ufile
    upload_cmd = '/root/filemgr/filemgr  --action mput --bucket opay-datalake --key oride/appsflyer/opay_event_log/dt={dt}/{file_name}  --file {tmp_file}'.format(
        dt=ds, file_name=file_name, tmp_file=tmp_file)

    os.system(upload_cmd)
    # clear tmp file
    clear_cmd = 'rm -f %s' % tmp_file
    os.system(clear_cmd)


import_opay_event_log = PythonOperator(
    task_id='import_opay_event_log',
    python_callable=import_opay_event,
    provide_context=True,
    dag=dag)

create_oride_global_daily_report = HiveOperator(
    task_id='create_oride_global_daily_report',
    hql="""
        CREATE TABLE IF NOT EXISTS `oride_global_daily_report`(
            `request_num` int,
            `request_num_lfw` int,
            `completed_num` int,
            `completed_num_lfw` int,
            `completed_drivers` int,
            `completed_users` int,
            `first_completed_users` int,
            `avg_take_time` int,
            `avg_duration` int,
            `avg_distance` int,
            `online_drivers` int,
            `avg_online_time` int,
            `btime_vs_otime` decimal(10,4),
            `active_users` int,
            `register_users` int,
            `register_drivers` int,
            `map_request_num` int,
            `avg_pickup_time` int,
            `oride_cllick_request_event_counter` int,
            `estimated_price_cllick_request_event_counter` int,
            `oride_cllick_request_event_counter_lfw` int,
            `estimated_price_cllick_request_event_counter_lfw` int,
            `take_num_lfw` int,
            `before_take_cancel_num_lfw` int,
            `after_take_cancel_num_lfw` int,
            `driver_cancel_num_lfw` int,
            `take_num` int,
            `before_take_cancel_num` int,
            `after_take_cancel_num` int,
            `driver_cancel_num` int,
            `pay_num` int,
            `pay_price_total` int,
            `pay_amount_total` int,
            `push_num` int,
            `online_pay_driver_num` int,
            `online_pay_order_num` int,
            `beckoning_num` int,
            `driect_ordernum` int,
            `driect_drivernum` int,
            `street_ordernum` int,
            `street_drivernum` int,
            `request_usernum` int,
            `new_user_request_num` int,
            `new_user_completed_num` int,
            `new_user_gmv` int,
            `completed_driver_online_time_total` bigint comment '完单司机在线总时长',
            `billing_time` bigint comment '总计费时长',
            `finished_num` int comment '订单状态为5的数量',
            `request_users` int comment '下单乘客数'
        )
        PARTITIONED BY (
            `dt` string)
        STORED AS PARQUET
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_global_daily_report = HiveOperator(
    task_id='insert_oride_global_daily_report',
    hql="""
        ALTER TABLE oride_source.appsflyer_opay_event_log ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}');
        ALTER TABLE oride_global_daily_report DROP IF EXISTS PARTITION (dt = '{{ ds }}');
        -- 近4周event
        with lfw_event_data as (
            SELECT
                '{{ ds }}' as dt,
                sum(case when event_name='choose_end_point_click' then 1 end)/count(DISTINCT dt) as oride_cllick_request_event_counter_lfw,
                sum(case when event_name='request_a_ride_show' then 1 end)/count(DISTINCT dt) as estimated_price_cllick_request_event_counter_lfw,
                sum(case when event_name='request_a_ride_click' then 1 end)/count(DISTINCT dt) as  request_a_ride_click_lfw
            FROM
                oride_dw.dwd_oride_client_event_detail_hi
            WHERE
                dt BETWEEN '{{ macros.ds_add(ds, -28) }}' AND '{{ macros.ds_add(ds, -1) }}' and app_version>='4.4.405' 
                AND from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') = from_unixtime(unix_timestamp('{{ ds }}', 'yyyy-MM-dd'),'u') and
                from_unixtime(cast(event_time as int),'yyyy-MM-dd') BETWEEN '{{ macros.ds_add(ds, -28) }}' AND '{{ macros.ds_add(ds, -1) }}'
                AND from_unixtime(cast(event_time as int),'u') = from_unixtime(unix_timestamp('{{ ds }}', 'yyyy-MM-dd'),'u') 
        ),
        -- event数据
        event_data as (
        SELECT
                dt,
                sum(case when event_name='choose_end_point_click' then 1 end) as oride_cllick_request_event_counter,
                sum(case when event_name='request_a_ride_show' then 1 end) as estimated_price_cllick_request_event_counter,
                sum(case when event_name='request_a_ride_click' then 1 end)/count(DISTINCT dt) as  request_a_ride_click
            FROM
                oride_dw.dwd_oride_client_event_detail_hi
            WHERE
                dt='{{ ds }}' and app_version>='4.4.405' and from_unixtime(cast(event_time as int),'yyyy-MM-dd')='{{ ds }}'
            GROUP BY
                dt
        ),
        -- 近4周数据
        lfw_data as (
            SELECT
               dt,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                , id, null)
                )/4 as request_num_lfw,
                count(
                if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and (status=4 or status=5)
                , id, null)
                )/4 as completed_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and (driver_id>0)
                , id, null)
                )/4 as take_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and (driver_id=0)
                    and (status=6)
                , id, null)
                )/4 as before_take_cancel_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and driver_id>0
                    and status=6
                , id, null)
                )/4 as after_take_cancel_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and driver_id>0
                    and status=6
                    and cancel_role=2
                , id, null)
                )/4 as driver_cancel_num_lfw
            FROM
                oride_dw_ods.ods_sqoop_base_data_order_df
            WHERE
                dt='{{ ds }}' and city_id !=999001
            GROUP BY dt
        ),
        -- 订单基础表
        order_base as (
            SELECT
                *
            FROM
                oride_dw_ods.ods_sqoop_base_data_order_df
            WHERE
                dt='{{ ds }}'
                AND city_id != 999001
                AND from_unixtime(create_time, 'yyyy-MM-dd')=dt
        ),
        -- 完单司机在线时长
        completed_driver_online as (
            SELECT
                t1.dt,
                SUM(nvl(t1.do_range, 0) + nvl(t2.driver_freerange, 0)) AS do_range
            FROM
                (
                    -- 完单司机做单时长
                    SELECT
                        do.dt,
                        do.driver_id,
                        sum(
                            CASE do.status
                                WHEN 4 THEN abs(do.arrive_time-do.take_time)
                                WHEN 5 THEN abs(do.finish_time-do.take_time)
                                WHEN 6 THEN abs(do.cancel_time-do.take_time)
                                ELSE 0
                            END
                         )  as do_range
                    FROM
                        (
                            -- 完单司机
                            SELECT
                                distinct driver_id
                            FROM
                                order_base
                            WHERE
                                status in (4,5)
                        ) dd
                        INNER JOIN order_base do ON do.driver_id=dd.driver_id
                    GROUP BY do.dt,do.driver_id
                ) t1
                INNER JOIN
                (
                    -- 空闲时长
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_log_oride_driver_timerange
                    WHERE
                        dt='{{ ds }}'
                ) t2 ON t2.driver_id=t1.driver_id
            GROUP BY t1.dt
        ),
        -- 订单数据
        order_data as (
            SELECT
                do.dt,
                count(DISTINCT do.id) as request_num,
                count(DISTINCT if(do.status=4 or do.status=5, do.id, null)) as completed_num,
                count(DISTINCT if(do.status=4 or do.status=5, do.driver_id, null)) as completed_drivers,
                nvl(avg(if(do.take_time>0 and (do.status=4 or do.status=5), do.take_time-do.create_time, null)),0) as avg_take_time,
                nvl(avg(if(do.pickup_time>0 and (do.status=4 or do.status=5), do.pickup_time-do.take_time, null)),0) as avg_pickup_time,
                nvl(avg(if(do.duration>0, do.duration, null)),0) as avg_duration,
                nvl(round(sum(if(do.status = 4 or do.status =5,do.distance, 0))/count(if(do.status=4 or do.status=5, do.id, null)),0),0) as avg_distance,
                SUM(do.duration) as total_duration,
                count(DISTINCT if(do.status=4 or do.status=5, do.user_id, null)) as completed_users,
                count(DISTINCT if((do.status=4 or do.status=5) and old_user.user_id is null, do.user_id, null)) as first_completed_users,
                count(DISTINCT if(do.driver_id>0, do.id, null)) as take_num,
                count(DISTINCT if(do.driver_id=0 and do.status=6, do.id, null)) as before_take_cancel_num,
                count(DISTINCT if(do.driver_id>0 and do.status=6, do.id, null)) as after_take_cancel_num,
                count(DISTINCT if(do.cancel_role=2 and do.status=6, do.id, null)) as driver_cancel_num,
                count(DISTINCT if(dop.status=1, do.id, null)) as pay_num,
                SUM(if(dop.status=1, dop.price, 0)) as pay_price_total,
                SUM(if(dop.status=1, dop.amount, 0)) as pay_amount_total,
                COUNT(DISTINCT if(dop.status=1 and (dop.mode=2 or dop.mode=3), dop.driver_id, null)) as online_pay_driver_num,
                COUNT(DISTINCT if(dop.status=1 and (dop.mode=2 or dop.mode=3), dop.id, null)) as online_pay_order_num,
                SUM(if(do.arrive_time>0 and do.status in (4,5), do.arrive_time-do.pickup_time, 0)) as billing_time,
                count(distinct if(do.serv_type=99 and (do.status=4 or do.status=5), do.id, null)) as beckoning_num,
                count(distinct if(do.driver_serv_type=1 and (do.status=4 or do.status=5), do.id, null)) as driect_ordernum,
                count(distinct if(do.driver_serv_type=1 and (do.status=4 or do.status=5), do.driver_id, null)) as driect_drivernum,
                count(distinct if(do.driver_serv_type=2 and (do.status=4 or do.status=5), do.id, null)) as street_ordernum,
                count(distinct if(do.driver_serv_type=2 and (do.status=4 or do.status=5), do.driver_id, null)) as street_drivernum,
                count(distinct do.user_id) as request_usernum,
                sum(if((do.status=4 or do.status=5) and do.serv_type = 3,do.pax_num,0)) trike_complete_passengernum,
                count(distinct if(dop.id is not null and dop.status=1 and (dop.mode = 2 or dop.mode = 3) and do.serv_type = 3,do.user_id,null)) trike_online_pay_passengernum,
                count(distinct if(dop.id is not null and dop.status=1 and do.serv_type = 3,do.user_id,null)) trike_pay_passengernum,
                count(DISTINCT if(do.status=5, do.id, null)) as finished_num,
                COUNT(DISTINCT if(dop.status in (0,2) and dop.mode=2 , dop.id, null)) as opay_pay_filed_order_num,
                COUNT(DISTINCT if(dop.mode=2 , dop.id, null)) as opay_pay_order_num
            FROM
                order_base do
                LEFT JOIN
                (
                    SELECT
                        distinct user_id
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_df
                    WHERE
                        dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd')<dt and status in (4,5)
                ) old_user on old_user.user_id=do.user_id
                LEFT JOIN
                (
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_payment_df
                    WHERE
                        dt='{{ ds }}'
                ) dop on dop.id=do.id and dop.dt=do.dt
            GROUP BY do.dt
        ),
        -- 用户数据
        user_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as register_users,
                sum(if(from_unixtime(login_time, 'yyyy-MM-dd')=dt, 1, 0)) as active_users
            FROM
                oride_dw_ods.ods_sqoop_base_data_user_extend_df
            WHERE
                dt='{{ ds }}'
            GROUP BY dt
        ),
        -- 所有数据表
        all_driver_data as (
            SELECT
                dt,
                id,
                register_time
            FROM
                oride_dw_ods.ods_sqoop_base_data_driver_extend_df
            WHERE
                dt='{{ ds }}' AND city_id!=999001
        ),
        -- 注册司机数据
        driver_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as register_drivers
            FROM
                all_driver_data
             GROUP BY dt
        ),
        -- 司机在线数据
        online_data as (
            SELECT
                odt.dt,
                count(distinct odt.driver_id) as online_drivers,
                SUM(odt.driver_onlinerange) as total_online_time
            FROM
                (
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_log_oride_driver_timerange
                    WHERE
                        dt='{{ ds }}'
                ) odt
                INNER JOIN
                all_driver_data dde on dde.id=odt.driver_id and dde.dt=odt.dt
            GROUP BY odt.dt
        ),
        -- 地图调用数据
        map_data as (
            SELECT
                dt,
                count(1) as map_request_num
            FROM
                 oride_source.server_magic
            WHERE
                dt='{{ ds }}' and event_name in ('googlemap_directions', 'googlemap_nearbysearch', 'googlemap_autocomplete', 'googlemap_details', 'googlemap_geocode')
            GROUP BY dt
        ),
        -- push 数据
        push_data as (
            select 
                dt,
                count(distinct(order_id)) push_num
            from oride_dw.dwd_oride_order_push_driver_detail_di
            where dt = '{{ ds }}' and success = 1
            group by dt
        ),
        -- 新用户数据
        new_user_data as (
            SELECT
                u.dt,
                COUNT(o.id) AS new_user_request_num,
                COUNT(if(o.status=4 or o.status=5, o.id, NULL)) AS new_user_completed_num,
                SUM(if(o.status=4 or o.status=5, p.price, 0)) AS new_user_gmv
            FROM
                (
                    SELECT
                        dt,
                        id as user_id
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_user_extend_df
                    WHERE
                        dt='{{ ds }}' AND from_unixtime(register_time, 'yyyy-MM-dd')=dt
                ) u
                INNER JOIN
                (
                    SELECT
                        id,
                        user_id,
                        status
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_df
                    WHERE
                        dt='{{ ds }}' AND from_unixtime(create_time, 'yyyy-MM-dd')=dt
                ) o ON o.user_id=u.user_id
                LEFT JOIN
                (
                    SELECT
                        id,
                        price
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_payment_df
                    WHERE
                        dt='{{ ds }}'
                ) p ON p.id=o.id
            GROUP BY u.dt
        )
        INSERT OVERWRITE TABLE oride_global_daily_report PARTITION (dt = '{{ ds }}')
        SELECT
            od.request_num,
            nvl(lf.request_num_lfw,0),
            od.completed_num,
            nvl(lf.completed_num_lfw,0),
            od.completed_drivers,
            od.completed_users,
            od.first_completed_users,
            od.avg_take_time,
            od.avg_duration,
            od.avg_distance,
            nvl(old.online_drivers,0),
            nvl(old.total_online_time/old.online_drivers,0) as avg_online_time,
            nvl(od.billing_time/old.total_online_time,0) as btime_vs_otime,
            nvl(ud.active_users,0),
            nvl(ud.register_users,0),
            nvl(dd.register_drivers,0),
            nvl(md.map_request_num,0) as map_request_num,
            od.avg_pickup_time,
            nvl(ed.oride_cllick_request_event_counter,0),
            nvl(ed.estimated_price_cllick_request_event_counter,0),
            nvl(led.oride_cllick_request_event_counter_lfw,0),
            nvl(led.estimated_price_cllick_request_event_counter_lfw,0),
            nvl(lf.take_num_lfw,0),
            nvl(lf.before_take_cancel_num_lfw,0),
            nvl(lf.after_take_cancel_num_lfw,0),
            nvl(lf.driver_cancel_num_lfw,0),
            od.take_num,
            od.before_take_cancel_num,
            od.after_take_cancel_num,
            od.driver_cancel_num,
            od.pay_num,
            od.pay_price_total,
            od.pay_amount_total,
            nvl(pd.push_num,0),
            od.online_pay_driver_num,
            od.online_pay_order_num,
            od.beckoning_num,
            od.driect_ordernum,
            od.driect_drivernum,
            od.street_ordernum,
            od.street_drivernum,
            od.request_usernum,
            od.trike_complete_passengernum,
            od.trike_online_pay_passengernum,
            od.trike_pay_passengernum,
            nud.new_user_request_num,
            nud.new_user_completed_num,
            nud.new_user_gmv,
            cdo.do_range,
            od.billing_time,
            od.finished_num,
            od.request_usernum,
            od.opay_pay_filed_order_num,
            od.opay_pay_order_num,
            nvl(led.request_a_ride_click_lfw,0) request_a_ride_click_lfw,
            nvl(ed.request_a_ride_click,0) request_a_ride_click
            
        FROM
            order_data od
            LEFT JOIN lfw_data lf on lf.dt=od.dt
            LEFT JOIN online_data old on old.dt=od.dt
            LEFT JOIN user_data ud on ud.dt=od.dt
            LEFT JOIN driver_data dd on dd.dt=od.dt
            LEFT JOIN map_data md on md.dt=od.dt
            LEFT JOIN event_data ed on ed.dt=od.dt
            LEFT JOIN lfw_event_data led on led.dt=od.dt
            LEFT JOIN push_data pd on pd.dt=od.dt
            LEFT JOIN new_user_data nud on nud.dt=od.dt
            LEFT JOIN completed_driver_online cdo ON cdo.dt=od.dt
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_order_city_daily_report = HiveOperator(
    task_id='insert_oride_order_city_daily_report',
    hql="""
    
        set hive.auto.convert.join = false;
        ALTER TABLE oride_source.appsflyer_opay_event_log ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}');
        ALTER TABLE oride_order_city_daily_report DROP IF EXISTS PARTITION (dt = '{{ ds }}');



        -- 近4周event
        with lfw_event_data as (
            SELECT
                '{{ ds }}' as dt,
                '' as city_id,
                SUM(oride_cllick_request_event_counter)/count(DISTINCT dt) as oride_cllick_request_event_counter_lfw,
                SUM(estimated_price_cllick_request_event_counter)/count(DISTINCT dt) as estimated_price_cllick_request_event_counter_lfw
            FROM
                oride_source.appsflyer_opay_event_log
            WHERE
                dt BETWEEN '{{ macros.ds_add(ds, -28) }}' AND '{{ macros.ds_add(ds, -1) }}'
                AND from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') = from_unixtime(unix_timestamp('{{ ds }}', 'yyyy-MM-dd'),'u')

        ),
        -- event数据
        event_data as (
            SELECT
                dt,
                '' as city_id,
                SUM(oride_cllick_request_event_counter) as oride_cllick_request_event_counter,
                SUM(estimated_price_cllick_request_event_counter) as estimated_price_cllick_request_event_counter
            FROM
                oride_source.appsflyer_opay_event_log
            WHERE
                dt='{{ ds }}'
            GROUP BY
                dt
        ),


        -- 城市天气数据
        weather_city_data as (
            select 
            '{{ ds }}' dt,
            t.city city,
            t.weather weather
            from 
            (
                select 
                t.city,
                t.weather,
                row_number() over(partition by t.city ORDER BY t.counts DESC) order_id
                from 
                (
                    select 
                    city,
                    weather,
                    count(1) counts
                    from oride_dw_ods.ods_sqoop_base_weather_per_10min_df where dt = '{{ ds }}'
                    and daliy = '{{ ds }}'
                    group by city,weather
                ) t
            ) t 
            where t.order_id = 1
        ),

        --城市下雨天气订单
        weather_city_order_data as (
            select 
            t.city,
            substring(t.dt,1,10) dt,
            sum(if(t.ride_num is null,0,t.ride_num)) rain_order_num
            from 
            (
                select 
                s.city,
                s.run_time dt,
                s.mins,
                d.ride_num
                from 
                (
                    select 
                        city,
                        from_unixtime(unix_timestamp(run_time),'yyyy-MM-dd HH') run_time,
                        minute(from_unixtime(unix_timestamp(run_time))) mins
                    from
                        oride_dw_ods.ods_sqoop_base_weather_per_10min_df
                    where
                        dt = '{{ ds }}'
                        and weather in ('Thundershower','Light rain','Rain','Thunderstorm','A shower')
                        and daliy = '{{ ds }}'
                ) s 
                join 
                (
                    select
                    c.name city_name,
                    t.time time,
                    t.mins * 10 mins,
                    count(t.id) ride_num
                    from  
                        (
                            select
                                id,
                                city_id,
                                from_unixtime(create_time,'yyyy-MM-dd HH') as time,
                                floor(cast(minute(from_unixtime(create_time)) as int) / 10) as mins
                            from
                                oride_dw_ods.ods_sqoop_base_data_order_df
                            where
                                dt= '{{ ds }}' and from_unixtime(create_time,'yyyy-MM-dd') = '{{ ds }}'
                        ) t 
                    join
                    (
                        SELECT
                            *
                        FROM
                            oride_dw_ods.ods_sqoop_base_data_city_conf_df
                        WHERE
                            dt= '{{ ds }}'

                    ) c on c.dt = '{{ ds }}' and t.city_id = c.id
                    group by time,t.mins,c.name
                ) d on lower(s.city) = lower(d.city_name) and s.run_time = d.time
                    and d.mins = s.mins
            ) t 
            group by t.city,substring(t.dt,1,10)
        ),


        -- 近4周数据
        lfw_data as (
            SELECT
               dt,
               city_id,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                , id, null)
                )/4 as request_num_lfw,
                count(
                if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and (status=4 or status=5)
                , id, null)
                )/4 as completed_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and (driver_id>0)
                , id, null)
                )/4 as take_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and (driver_id=0)
                    and (status=6)
                , id, null)
                )/4 as before_take_cancel_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and driver_id>0
                    and status=6
                , id, null)
                )/4 as after_take_cancel_num_lfw,
               count(
               if(
                    datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))>0
                    and datediff(dt, from_unixtime(create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(create_time,'u') = from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u')
                    and driver_id>0
                    and status=6
                    and cancel_role=2
                , id, null)
                )/4 as driver_cancel_num_lfw
            FROM
                oride_dw_ods.ods_sqoop_base_data_order_df
            WHERE
                dt='{{ ds }}' and city_id !=999001
            GROUP BY dt,city_id
        ),
        -- 当日订单数据
        order_base as (
            SELECT
                *
            FROM
                oride_dw_ods.ods_sqoop_base_data_order_df
            WHERE
                dt='{{ ds }}'
                AND city_id != 999001
                AND from_unixtime(create_time, 'yyyy-MM-dd')=dt
        ),
        -- 当日有效订单数据
        effective_order_data as (
            SELECT
               id,
                if(status in (4,5), 1, if(id=id2, 1,
                    if(abs(create_time2-create_time)<=1800 and
                    2*asin(sqrt(pow(sin((start_lat*pi()/180.0-start_lat2*pi()/180.0)/2),2) + cos(start_lat*pi()/180.0)*cos(start_lat2*pi()/180.0)*pow(sin((start_lng*pi()/180.0-start_lng2*pi()/180.0)/2),2)))*6378137 <= 1000 and
                    2*asin(sqrt(pow(sin((end_lat*pi()/180.0-end_lat2*pi()/180.0)/2),2) + cos(end_lat*pi()/180.0)*cos(end_lat2*pi()/180.0)*pow(sin((end_lng*pi()/180.0-end_lng2*pi()/180.0)/2),2)))*6378137 <= 1000, 0, 1
                    ))
                ) as is_effective
            FROM (
                SELECT
                    city_id,
                    dt,
                    id,
                    driver_id,
                    user_id,
                    start_lng,
                    start_lat,
                    end_lng,
                    end_lat,
                    create_time,
                    arrive_time,
                    status,
                    lead(create_time,1,create_time) over(partition by user_id order by create_time) create_time2,
                    lead(start_lng,1,0) over(PARTITION BY user_id ORDER BY create_time) start_lng2,
                    lead(start_lat,1,0) over(PARTITION BY user_id ORDER BY create_time) start_lat2,
                    lead(end_lng,1,0) over(PARTITION BY user_id ORDER BY create_time) end_lng2,
                    lead(end_lat,1,0) over(PARTITION BY user_id ORDER BY create_time) end_lat2,
                    lead(id,1,id) over(PARTITION BY user_id ORDER BY create_time) id2
                FROM
                    order_base
                ) as t
        ),
        -- 订单数据
        order_data as (
            SELECT
                do.dt,
                do.city_id,
                count(DISTINCT do.id) as request_num,
                count(DISTINCT if(do.status=4 or do.status=5, do.id, null)) as completed_num,
                count(DISTINCT if(do.status=4 or do.status=5, do.driver_id, null)) as completed_drivers,
                avg(if(do.take_time>0 and (do.status=4 or do.status=5), do.take_time-do.create_time, null)) as avg_take_time,
                avg(if(do.pickup_time>0 and (do.status=4 or do.status=5), do.pickup_time-do.take_time, null)) as avg_pickup_time,
                avg(if(do.duration>0, do.duration, null)) as avg_duration,
                round(sum(if(do.status = 4 or do.status =5,do.distance, 0))/count(if(do.status=4 or do.status=5, do.id, null)),0) as avg_distance,
                SUM(do.duration) as total_duration,
                count(DISTINCT if(do.status=4 or do.status=5, do.user_id, null)) as completed_users,
                count(DISTINCT if((do.status=4 or do.status=5) and old_user.user_id is null, do.user_id, null)) as first_completed_users,
                count(DISTINCT if(do.driver_id>0, do.id, null)) as take_num,
                count(DISTINCT if(do.driver_id=0 and do.status=6, do.id, null)) as before_take_cancel_num,
                count(DISTINCT if(do.driver_id>0 and do.status=6, do.id, null)) as after_take_cancel_num,
                count(DISTINCT if(do.cancel_role=2 and do.status=6, do.id, null)) as driver_cancel_num,
                count(DISTINCT if(dop.status=1, do.id, null)) as pay_num,
                SUM(if(dop.status=1, dop.price, 0)) as pay_price_total,
                SUM(if(dop.status=1, dop.amount, 0)) as pay_amount_total,
                COUNT(DISTINCT if(dop.status=1 and (dop.mode=2 or dop.mode=3), dop.driver_id, null)) as online_pay_driver_num,
                COUNT(DISTINCT if(dop.status=1 and (dop.mode=2 or dop.mode=3), dop.id, null)) as online_pay_order_num,
                SUM(if(do.arrive_time>0, do.arrive_time-do.pickup_time, 0)) as billing_time,
                count(distinct if(do.serv_type=99 and (do.status=4 or do.status=5), do.id, null)) as beckoning_num,
                count(distinct if(do.driver_serv_type=1 and (do.status=4 or do.status=5), do.id, null)) as driect_ordernum,
                count(distinct if(do.driver_serv_type=1 and (do.status=4 or do.status=5), do.driver_id, null)) as driect_drivernum,
                count(distinct if(do.driver_serv_type=2 and (do.status=4 or do.status=5), do.id, null)) as street_ordernum,
                count(distinct if(do.driver_serv_type=2 and (do.status=4 or do.status=5), do.driver_id, null)) as street_drivernum,
                count(distinct do.user_id) as request_usernum,
                count(if(eod.is_effective=1, eod.id, null)) as effective_order_num,
                COUNT(DISTINCT if(dop.status=1 and (dop.mode=2 or dop.mode=3), do.user_id, null)) as online_pay_user_num
            FROM
                order_base do
                LEFT JOIN
                (
                    SELECT
                        distinct user_id
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_df
                    WHERE
                        dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd')<dt and status in (4,5)
                ) old_user on old_user.user_id=do.user_id
                LEFT JOIN
                (
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_payment_df
                    WHERE
                       dt='{{ ds }}'
                ) dop on dop.id=do.id and dop.dt=do.dt
                LEFT JOIN effective_order_data eod ON eod.id=do.id
            GROUP BY do.dt,do.city_id
        ),

        -- push 数据
        push_data as (
            select 
                dt,
                city_id city_id,
                count(distinct(order_id)) push_num
            from
                oride_dw.dwd_oride_order_push_driver_detail_di
            where
                dt = '{{ ds }}' and success = 1
            group by dt,city_id
        ),
        -- 用户数据
        user_data as (
            SELECT
                dt,
                city_id,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as register_users,
                sum(if(from_unixtime(login_time, 'yyyy-MM-dd')=dt, 1, 0)) as active_users
            FROM
                oride_dw_ods.ods_sqoop_base_data_user_extend_df
            WHERE
                dt='{{ ds }}'
            GROUP BY dt, city_id
        )


        INSERT OVERWRITE TABLE oride_order_city_daily_report PARTITION (dt = '{{ ds }}')
        select 
            t.name,
            t.weather,
            t.rain_order_num,
            t.request_num,
            t.request_num_lfw,
            t.completed_num,
            t.completed_num_lfw,
            t.completed_drivers,
            t.completed_users,
            t.first_completed_users,
            t.avg_take_time,
            t.avg_duration,
            t.avg_distance,
            t.avg_pickup_time,
            t.oride_cllick_request_event_counter,
            t.estimated_price_cllick_request_event_counter,
            t.oride_cllick_request_event_counter_lfw,
            t.estimated_price_cllick_request_event_counter_lfw,
            t.take_num_lfw,
            t.before_take_cancel_num_lfw,
            t.after_take_cancel_num_lfw,
            t.driver_cancel_num_lfw,
            t.take_num,
            t.before_take_cancel_num,
            t.after_take_cancel_num,
            t.driver_cancel_num,
            t.pay_num,
            t.pay_price_total,
            t.pay_amount_total,
            t.push_num,
            t.online_pay_driver_num,
            t.online_pay_order_num,
            t.beckoning_num,
            t.driect_ordernum,
            t.driect_drivernum,
            t.street_ordernum,
            t.street_drivernum,
            t.request_usernum,
            t.effective_order_num,
            t.online_pay_user_num,
            t.active_users
        from
        (
            SELECT
                c.name,
                nvl(wcd.weather,'-') weather,
                nvl(wod.rain_order_num,0) rain_order_num,
                od.request_num,
                nvl(lf.request_num_lfw,0) request_num_lfw,
                od.completed_num,
                nvl(lf.completed_num_lfw,0) completed_num_lfw,
                od.completed_drivers,
                od.completed_users,
                od.first_completed_users,
                od.avg_take_time,
                od.avg_duration,
                od.avg_distance,
                od.avg_pickup_time,
                nvl(ed.oride_cllick_request_event_counter,0) oride_cllick_request_event_counter,
                nvl(ed.estimated_price_cllick_request_event_counter,0) estimated_price_cllick_request_event_counter,
                nvl(led.oride_cllick_request_event_counter_lfw,0) oride_cllick_request_event_counter_lfw,
                nvl(led.estimated_price_cllick_request_event_counter_lfw,0) estimated_price_cllick_request_event_counter_lfw,
                nvl(lf.take_num_lfw,0) take_num_lfw,
                nvl(lf.before_take_cancel_num_lfw,0) before_take_cancel_num_lfw,
                nvl(lf.after_take_cancel_num_lfw,0) after_take_cancel_num_lfw,
                nvl(lf.driver_cancel_num_lfw,0) driver_cancel_num_lfw,
                od.take_num,
                od.before_take_cancel_num,
                od.after_take_cancel_num,
                od.driver_cancel_num,
                od.pay_num,
                od.pay_price_total,
                od.pay_amount_total,
                pd.push_num,
                od.online_pay_driver_num,
                od.online_pay_order_num,
                od.beckoning_num,
                od.driect_ordernum,
                od.driect_drivernum,
                od.street_ordernum,
                od.street_drivernum,
                od.request_usernum ,
                od.effective_order_num,
                od.online_pay_user_num,
                ud.active_users,
                row_number() over(partition by c.name order by request_num desc) order_id
            FROM
                order_data od
                join
                (
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_city_conf_df
                    WHERE
                        dt='{{ ds }}'
                ) c on od.city_id = c.id and c.dt = od.dt
                left join weather_city_data wcd on lower(wcd.city) = lower(c.name) and wcd.dt = od.dt
                left join weather_city_order_data wod on lower(wod.city) = lower(c.name) and wod.dt = od.dt
                LEFT JOIN lfw_data lf on lf.dt=od.dt and lf.city_id = od.city_id
                LEFT JOIN event_data ed on ed.dt=od.dt and ed.city_id = od.city_id
                LEFT JOIN lfw_event_data led on led.dt=od.dt and led.city_id = od.city_id
                LEFT JOIN push_data pd on pd.dt=od.dt and pd.city_id = od.city_id
                LEFT JOIN user_data ud ON ud.dt=od.dt and ud.city_id = od.city_id
        ) t 
        where t.order_id = 1
        """,
    schema='oride_bi',
    dag=dag)

create_oride_global_city_serv_daily_report = HiveOperator(
    task_id='create_oride_global_city_serv_daily_report',
    hql="""
        CREATE TABLE IF NOT EXISTS `oride_global_city_serv_daily_report`(
            `city_id` bigint COMMENT '所属城市ID',
            `driver_serv_type` tinyint comment '1:direct 2: stree',
            `completed_num` int comment '完单数',
            `completed_num_lfw` int comment '完单数（近4周均值）',
            `completed_users` int comment '完单用户数',
            `completed_drivers` int comment '完单司机数',
            `online_time_total` bigint comment '总在线时长',
            `online_drivers` int comment '总在线司机数',
            `billing_time_total` bigint comment '总计费时长',
            `push_num` int comment '推送订单数',
            `push_drivers` int comment '推送司机数',
            `register_drivers` int comment '注册司机数',
            `take_time_total` bigint comment '总应答时长（完单）',
            `pickup_time_total` bigint comment '总接驾时长（完单）',
            `distance_total` bigint comment '总送驾距离（完单）',
            `request_num` int,
            `request_num_lfw` int,
            `first_completed_users` int,
            `trike_complete_passengernum` int,
            `trike_online_pay_passengernum` int,
            `trike_pay_passengernum` int,
            `request_usernum` int,
            `new_user_completed_num` int comment '新用户完单数',
            `new_user_gmv` int comment '新用户gmv',
            `gmv` int comment 'gmv',
            `pay_num` int comment '支付订单数',
            `pay_price` bigint comment '应付金额',
            `pay_amount` bigint comment '实付金额',
            `b_subsidy` bigint comment 'B端补贴',
            `completed_driver_online_time_total` bigint comment '完单司机在线总时长',
            `finished_num` int comment '订单状态为5的数量',
            `order_score_num` int comment '评分为1、2的完单数'
        )
        PARTITIONED BY (
            `dt` string)
        STORED AS PARQUET
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_global_city_serv_daily_report = HiveOperator(
    task_id='insert_oride_global_city_serv_daily_report',
    hql="""
        ALTER TABLE oride_global_city_serv_daily_report DROP IF EXISTS PARTITION (dt = '{{ ds }}');
        
        WITH driver_dim as (
            select 
                id,
                city_id,
                serv_type
            from 

                oride_dw_ods.ods_sqoop_base_data_driver_extend_df
            WHERE
                dt='{{ ds }}'
        ),
        
        
        -- 近4周数据
         lfw_data as (
            SELECT
               o.dt,
               o.city_id,
               d.serv_type,
               count(
               if(
                    datediff(o.dt, from_unixtime(o.create_time, 'yyyy-MM-dd'))>0
                    and datediff(o.dt, from_unixtime(o.create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(o.create_time,'u') = from_unixtime(unix_timestamp(o.dt, 'yyyy-MM-dd'),'u')
                , o.id, null)
                )/4 as request_num_lfw,
               count(
                if(
                    datediff(o.dt, from_unixtime(o.create_time, 'yyyy-MM-dd'))>0
                    and datediff(o.dt, from_unixtime(o.create_time, 'yyyy-MM-dd'))<=28
                    and from_unixtime(o.create_time,'u') = from_unixtime(unix_timestamp(o.dt, 'yyyy-MM-dd'),'u')
                    and o.status in (4,5)
                , o.id, null)
                )/4 as completed_num_lfw
            FROM
                (
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_df
                    WHERE
                        dt='{{ ds }}'
                ) o
                inner join driver_dim d on d.city_id = o.city_id and d.id = o.driver_id
            GROUP BY o.dt,o.city_id,d.serv_type
        ),
        -- 订单基础表
        order_base as (
            SELECT
                *
            FROM
                oride_dw_ods.ods_sqoop_base_data_order_df
            WHERE
                dt='{{ ds }}'
                AND city_id != 999001
                AND from_unixtime(create_time, 'yyyy-MM-dd')=dt
        ),
        -- 完单司机在线时长
        completed_driver_online as (
            SELECT
                t1.dt,
                t3.city_id,
                t3.serv_type,
                SUM(nvl(t1.do_range, 0) + nvl(t2.driver_freerange, 0)) AS do_range
            FROM
                (
                    -- 完单司机做单时长
                    SELECT
                        do.dt,
                        do.driver_id,
                        sum(
                            CASE do.status
                                WHEN 4 THEN abs(do.arrive_time-do.take_time)
                                WHEN 5 THEN abs(do.finish_time-do.take_time)
                                WHEN 6 THEN abs(do.cancel_time-do.take_time)
                                ELSE 0
                            END
                         )  as do_range
                    FROM
                        (
                            -- 完单司机
                            SELECT
                                distinct driver_id
                            FROM
                                order_base
                            WHERE
                                status in (4,5)
                        ) dd
                        INNER JOIN order_base do ON do.driver_id=dd.driver_id
                    GROUP BY do.dt,do.driver_id
                ) t1
                INNER JOIN
                (
                    -- 空闲时长
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_log_oride_driver_timerange
                    WHERE
                        dt='{{ ds }}'
                ) t2 ON t2.driver_id=t1.driver_id
                INNER JOIN driver_dim t3 ON t3.id=t1.driver_id
            GROUP BY t1.dt,t3.city_id,t3.serv_type
        ),
        -- 订单打分
        order_comment as (
            SELECT
                *
            FROM
                oride_dw_ods.ods_sqoop_base_data_user_comment_df
            WHERE
                dt='{{ ds }}'
        ),
        -- 订单数据
        order_data as (
            SELECT
                do.dt,
                do.city_id,
                d.serv_type,
                COUNT(if(do.status in (4,5),do.id,null)) AS completed_num,
                SUM(if(do.arrive_time>0 and do.status in (4,5), do.arrive_time-do.pickup_time, 0)) as billing_time_total,
                SUM(if(do.status in (4,5),do.take_time-do.create_time,0)) as take_time_total,
                SUM(if(do.status in (4,5),do.pickup_time-do.take_time,0)) as pickup_time_total,
                sum(if(do.status in (4,5),do.distance,0)) as distance_total,
                count(DISTINCT if(do.status in (4,5),do.user_id,null)) as completed_users,
                count(DISTINCT if(do.status in (4,5),do.driver_id,null)) as completed_drivers,
                count(distinct do.user_id) as request_usernum,
                count(do.id) as request_num,
                count(DISTINCT if((do.status=4 or do.status=5) and old_user.user_id is null, do.user_id, null)) as first_completed_users,
                sum(if((do.status=4 or do.status=5) and do.serv_type = 3,do.pax_num,0)) trike_complete_passengernum,
                count(distinct if(dop.id is not null and dop.status=1 and (dop.mode = 2 or dop.mode = 3) and do.serv_type = 3,do.user_id,null)) trike_online_pay_passengernum,
                count(distinct if(dop.id is not null and dop.status=1 and do.serv_type = 3,do.user_id,null)) trike_pay_passengernum,
                sum(dop.price) as gmv,
                count(DISTINCT if(dop.status=1, dop.id, null)) as pay_num,
                sum(if(dop.status=1, dop.price, 0)) as pay_price,
                sum(if(dop.status=1, dop.amount, 0)) as pay_amount,
                sum((nvl(drr.amount,0)+nvl(dr.amount,0))) as b_subsidy,
                COUNT(if(do.status in (5),do.id,null)) AS finished_num,
                COUNT(if(do.status in (4,5) and oc.score in (1,2), do.id, null)) AS order_score_num
            FROM
                order_base do
                inner join driver_dim d on d.city_id = do.city_id and d.id = do.driver_id
                left join 
                (
                    SELECT
                        distinct user_id
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_df
                    WHERE
                        dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd')<'{{ ds }}' and status in (4,5)
                ) old_user on old_user.user_id=do.user_id
                LEFT JOIN
                (
                    SELECT
                      *
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_payment_df
                    WHERE
                      dt='{{ ds }}'

                ) dop on dop.id=do.id
                LEFT JOIN
                (
                    SELECT
                      order_id,
                      sum(amount) as amount
                    FROM
                      oride_dw_ods.ods_sqoop_base_data_driver_recharge_records_df
                    WHERE
                      dt='{{ ds }}' AND amount>0
                    group by order_id
                ) drr on drr.order_id=do.id
                LEFT JOIN
                (
                    SELECT
                      order_id,
                      sum(amount) as amount
                    FROM
                      oride_dw_ods.ods_sqoop_base_data_driver_reward_df
                    WHERE
                      dt='{{ ds }}'
                      group by order_id
                ) dr on dr.order_id=do.id
                LEFT JOIN order_comment oc ON oc.order_id=do.id
            GROUP BY do.dt,do.city_id,d.serv_type
        ),
        -- 司机数据
        driver_data as (
            SELECT
                dt,
                city_id,
                serv_type,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as register_drivers
            FROM
                oride_dw_ods.ods_sqoop_base_data_driver_extend_df
            WHERE
                dt='{{ ds }}'
            GROUP BY dt,city_id,serv_type
        ),
        -- 司机在线数据
        online_data as (
            SELECT
                odt.dt,
                d.city_id,
                d.serv_type,
                count(distinct odt.driver_id) as online_drivers,
                SUM(odt.driver_onlinerange) as online_time_total
            FROM
                (
                    SELECT
                        *
                    FROM
                        oride_dw_ods.ods_log_oride_driver_timerange
                    WHERE
                        dt='{{ ds }}'
                ) odt
                INNER JOIN
                driver_dim d on d.id = odt.driver_id
            GROUP BY odt.dt,d.city_id,d.serv_type
        ),
        -- 推送数据
        push_data as (
            select
                t.dt,
                d.city_id,
                d.serv_type,
                sum(t.order_num) push_num, -- 推送订单量
                count(t.driver_id) push_drivers -- 推送司机数
                from
                (
                    select
                        s.dt,
                        s.driver_id driver_id,
                        count(distinct(s.order_id)) order_num
                    from
                    (
                        select
                            dt,
                            order_id,
                            driver_id
                        from
                            oride_dw.dwd_oride_order_push_driver_detail_di
                        where
                            dt = '{{ ds }}' and success = 1
                    ) s
                    group by s.driver_id,s.dt
                ) t
                INNER JOIN
                driver_dim d on d.id = t.driver_id
            GROUP BY t.dt, d.city_id,d.serv_type
        ),
        -- 新用户数据
        new_user_data as (
            SELECT
                u.dt,
                dd.city_id,
                dd.serv_type,
                COUNT(if(o.status=4 or o.status=5, o.id, NULL)) AS new_user_completed_num,
                SUM(if(o.status=4 or o.status=5, p.price, 0)) AS new_user_gmv
            FROM
                (
                    SELECT
                        dt,
                        id as user_id
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_user_extend_df
                    WHERE
                        dt='{{ ds }}' AND from_unixtime(register_time, 'yyyy-MM-dd')=dt
                ) u
                INNER JOIN
                (
                    SELECT
                        id,
                        user_id,
                        status,
                        driver_id
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_df
                    WHERE
                        dt='{{ ds }}' AND from_unixtime(create_time, 'yyyy-MM-dd')=dt
                ) o ON o.user_id=u.user_id
                LEFT JOIN
                (
                    SELECT
                        id,
                        price
                    FROM
                        oride_dw_ods.ods_sqoop_base_data_order_payment_df
                    WHERE
                        dt='{{ ds }}'
                ) p ON p.id=o.id
                INNER JOIN driver_dim dd ON dd.id=o.driver_id
            GROUP BY u.dt, dd.city_id, dd.serv_type
        )
        INSERT OVERWRITE TABLE oride_global_city_serv_daily_report PARTITION (dt = '{{ ds }}')
        SELECT
            od.city_id,
            od.serv_type,
            nvl(od.completed_num,0),
            nvl(ld.completed_num_lfw,0),
            nvl(od.completed_users,0),
            nvl(od.completed_drivers,0),
            nvl(old.online_time_total,0),
            nvl(old.online_drivers,0),
            nvl(od.billing_time_total,0),
            nvl(pd.push_num,0),
            nvl(pd.push_drivers,0),
            nvl(dd.register_drivers,0),
            nvl(od.take_time_total,0),
            nvl(od.pickup_time_total,0),
            nvl(od.distance_total,0),
            nvl(od.request_num,0),
            nvl(ld.request_num_lfw,0),
            nvl(od.first_completed_users,0),
            nvl(od.trike_complete_passengernum,0),
            nvl(od.trike_online_pay_passengernum,0),
            nvl(od.trike_pay_passengernum,0),
            nvl(od.request_usernum,0),
            nvl(nud.new_user_completed_num, 0),
            nvl(nud.new_user_gmv, 0),
            nvl(od.gmv, 0),
            nvl(od.pay_num, 0),
            nvl(od.pay_price, 0),
            nvl(od.pay_amount, 0),
            nvl(od.b_subsidy, 0),
            cdo.do_range,
            od.finished_num,
            od.order_score_num
        FROM
            order_data od
            LEFT JOIN lfw_data ld ON ld.dt=od.dt AND ld.city_id=od.city_id AND ld.serv_type=od.serv_type
            LEFT JOIN driver_data dd ON dd.dt=od.dt AND dd.city_id=od.city_id AND dd.serv_type=od.serv_type
            LEFT JOIN online_data old ON old.dt=od.dt AND old.city_id=od.city_id AND old.serv_type=od.serv_type
            LEFT JOIN push_data pd ON pd.dt=od.dt AND pd.city_id=od.city_id AND pd.serv_type=od.serv_type
            LEFT JOIN new_user_data nud ON nud.dt=od.dt AND nud.city_id=od.city_id AND nud.serv_type=od.serv_type
            LEFT JOIN completed_driver_online cdo ON cdo.dt=od.dt AND cdo.city_id=od.city_id AND cdo.serv_type=od.serv_type
        """,
    schema='oride_bi',
    dag=dag)

def get_city_row(ds, all_completed_num):
    tr_fmt = '''
       <tr>{row}</tr>
    '''
    row_fmt = '''
         <!--{}{}-->
        <td>{}</td>
        <td>{}</td>
        <!--天气指标-->
        <td>{}</td>
        <td>{}</td>
        <!--关键指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--乘客指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
    '''
    sql = '''
        with all_data as (
            SELECT
                dt,
                SUM(rain_order_num) AS rain_order_num,
                SUM(request_num) AS request_num,
                SUM(request_num_lfw) AS request_num_lfw,
                SUM(effective_order_num) AS effective_order_num,
                SUM(pay_num) AS pay_num,
                SUM(completed_num) AS completed_num,
                SUM(completed_num_lfw) AS completed_num_lfw,
                SUM(active_users) AS active_users,
                SUM(request_usernum) AS request_usernum,
                SUM(completed_users) AS completed_users,
                SUM(online_pay_user_num) AS online_pay_user_num
            FROM
                oride_bi.oride_order_city_daily_report
            WHERE
                dt='{ds}'
            GROUP BY
                dt
        ),
        city_data as (
            SELECT
                *
            FROM
                oride_bi.oride_order_city_daily_report
            WHERE
                dt='{ds}'
        )
        -- 全部城市数据
        SELECT
            '1' as order_by,
            0 as city_id,
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            'All' as name,
            '-',
            concat(cast(nvl(round(rain_order_num/request_num*100, 1),0) as string), '%'),
            request_num,
            request_num_lfw,
            effective_order_num,
            pay_num,
            completed_num,
            completed_num_lfw,
            concat(cast(nvl(round(completed_num/request_num*100, 1),0) as string), '%'),
            concat(cast(nvl(round(completed_num_lfw/request_num_lfw*100, 1),0) as string), '%'),
            '100%',
            active_users,
            request_usernum,
            completed_users,
            concat(cast(nvl(round(online_pay_user_num/completed_users*100, 1),0) as string), '%')
        FROM
            all_data
        -- 分城市数据
        UNION
        SELECT
            '2' as order_by,
            td.id as city_id,
            from_unixtime(unix_timestamp(cd.dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            td.name as name,
            cd.weather,
            concat(cast(nvl(round(cd.rain_order_num/cd.request_num*100, 1),0) as string), '%'),
            cd.request_num,
            cd.request_num_lfw,
            cd.effective_order_num,
            cd.pay_num,
            cd.completed_num,
            cd.completed_num_lfw,
            concat(cast(nvl(round(cd.completed_num/cd.request_num*100, 1),0) as string), '%'),
            concat(cast(nvl(round(cd.completed_num_lfw/cd.request_num_lfw*100, 1),0) as string), '%'),
            concat(cast(nvl(round(cd.completed_num/{all_completed_num}*100, 1),0) as string), '%'),
            cd.active_users,
            cd.request_usernum,
            cd.completed_users,
            concat(cast(nvl(round(cd.online_pay_user_num/cd.completed_users*100, 1),0) as string), '%')
        FROM
            city_data cd
            INNER JOIN
            (
                SELECT
                    *
                FROM
                    oride_dw_ods.ods_sqoop_base_data_city_conf_df
                WHERE
                   dt='{ds}'
            ) td ON lower(cd.city) = lower(td.name)
         ORDER BY order_by ASC, city_id ASC
    '''.format(ds=ds, all_completed_num=all_completed_num)
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)
    return row_html

def get_serv_row(ds, driver_serv_type, all_completed_num):
    tr_fmt = '''
       <tr>{row}</tr>
    '''
    row_fmt = '''
        <!--{}{}-->
        <td>{}</td>
        <td>{}</td>
        <!--关键指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--供需关系-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--司机指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--体验指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--乘客指标-->
        <td>{}</td>
        <td>{}</td>
        <!--财务指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
    '''
    sql = '''
        with all_data as (
            SELECT
                dt,
                SUM(completed_num) as completed_num,
                AVG(completed_num_lfw) as completed_num_lfw,
                SUM(completed_users) as completed_users,
                SUM(completed_drivers) as completed_drivers,
                SUM(online_time_total) as online_time_total,
                SUM(online_drivers) as online_drivers,
                SUM(billing_time_total) as billing_time_total,
                SUM(push_num) as push_num,
                SUM(push_drivers) as push_drivers,
                SUM(register_drivers) as register_drivers,
                SUM(take_time_total) as take_time_total,
                SUM(pickup_time_total) as pickup_time_total,
                SUM(distance_total) as distance_total,
                SUM(new_user_completed_num) as new_user_completed_num,
                SUM(new_user_gmv) as new_user_gmv,
                SUM(gmv) as gmv,
                SUM(pay_num) as pay_num,
                SUM(pay_price) as pay_price,
                SUM(pay_amount) as pay_amount,
                SUM(b_subsidy) as b_subsidy,
                SUM(completed_driver_online_time_total) as completed_driver_online_time_total,
                SUM(finished_num) as finished_num,
                SUM(order_score_num) as order_score_num
            FROM
                oride_bi.oride_global_city_serv_daily_report
            WHERE
                dt='{ds}' AND driver_serv_type={driver_serv_type}
            GROUP BY
                dt
        ), all_city_data as (
            SELECT
                dt,
                city_id,
                SUM(completed_num) as completed_num
            FROM
                 oride_bi.oride_global_city_serv_daily_report
            WHERE
                 dt='{ds}'
            GROUP BY
                 dt,city_id
        ),city_data as (
            SELECT
                *
            FROM
                oride_bi.oride_global_city_serv_daily_report
            WHERE
                dt='{ds}' AND driver_serv_type={driver_serv_type}
        )
        -- 全部城市数据
        SELECT
            '1' as order_by,
            0 as city_id,
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            'All' as name,
            completed_num,
            cast(nvl(completed_num_lfw, 0) as int),
            nvl(finished_num, '-'),
            concat(cast(nvl(round(completed_num/{all_completed_num}*100, 1),0) as string), '%'),
            '100%',
            completed_users,
            completed_drivers,
            if(completed_driver_online_time_total is null, '-', round(completed_driver_online_time_total/3600/completed_drivers, 1)),
            concat(cast(nvl(round(billing_time_total/completed_driver_online_time_total*100, 1),0) as string), '%'),
            nvl(round(push_num/push_drivers, 1),0),
            register_drivers,
            online_drivers,
            nvl(round(completed_num/completed_drivers, 1),0),
            if(completed_driver_online_time_total is null, '-', round(completed_num/(completed_driver_online_time_total/3600), 1)) as tph,
            if(order_score_num is null, '-', concat(cast(nvl(round(order_score_num/completed_num*10000, 1),0) as string), '‱')),
            cast(nvl(round(take_time_total/completed_num),0) as int),
            cast(nvl(round(pickup_time_total/completed_num),0) as int),
            cast(nvl(round(billing_time_total/completed_num),0) as int),
            cast(nvl(round(distance_total/completed_num),0) as int),
            nvl(new_user_completed_num,0),
            nvl(new_user_gmv,0),
            nvl(round(pay_price/pay_num),0),
            concat(cast(nvl(round(b_subsidy/gmv*100, 1),0) as string), '%'),
            concat(cast(nvl(round((pay_price-pay_amount)/gmv*100, 1),0) as string), '%')
        FROM
            all_data
        -- 分城市数据
        UNION
        SELECT
            '2' as order_by,
            td.id as city_id,
            from_unixtime(unix_timestamp(cd.dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            td.name as name,
            cd.completed_num,
            cast(nvl(cd.completed_num_lfw, 0) as int),
            nvl(cd.finished_num, '-'),
            concat(cast(nvl(round(cd.completed_num/acd.completed_num*100, 1),0) as string), '%'),
            concat(cast(nvl(round(cd.completed_num/ad.completed_num*100, 1),0) as string), '%'),
            cd.completed_users,
            cd.completed_drivers,
            if(cd.completed_driver_online_time_total is null, '-', round(cd.completed_driver_online_time_total/3600/cd.completed_drivers, 1)),
            concat(cast(nvl(round(cd.billing_time_total/cd.completed_driver_online_time_total*100, 1),0) as string), '%'),
            nvl(round(cd.push_num/cd.push_drivers, 1),0),
            cd.register_drivers,
            cd.online_drivers,
            nvl(round(cd.completed_num/cd.completed_drivers, 1),0),
            if(cd.completed_driver_online_time_total is null, '-', round(cd.completed_num/(cd.completed_driver_online_time_total/3600), 1)) as tph,
            if(cd.order_score_num is null, '-', concat(cast(nvl(round(cd.order_score_num/cd.completed_num*10000, 1),0) as string), '‱')),
            cast(nvl(round(cd.take_time_total/cd.completed_num),0) as int),
            cast(nvl(round(cd.pickup_time_total/cd.completed_num),0) as int),
            cast(nvl(round(cd.billing_time_total/cd.completed_num),0) as int),
            cast(nvl(round(cd.distance_total/cd.completed_num),0) as int),
            nvl(cd.new_user_completed_num,0),
            nvl(cd.new_user_gmv,0),
            nvl(round(cd.pay_price/cd.pay_num),0),
            concat(cast(nvl(round(cd.b_subsidy/cd.gmv*100, 1),0) as string), '%'),
            concat(cast(nvl(round((cd.pay_price-cd.pay_amount)/cd.gmv*100, 1),0) as string), '%')
        FROM
            city_data cd
            INNER JOIN all_data ad ON ad.dt=cd.dt
            INNER JOIN all_city_data acd ON acd.city_id=cd.city_id AND acd.dt=cd.dt
            INNER JOIN
            (
                SELECT
                    *
                FROM
                    oride_dw_ods.ods_sqoop_base_data_city_conf_df
                WHERE
                   dt='{ds}'
            ) td ON td.id=cd.city_id
        ORDER BY order_by ASC, city_id ASC
    '''.format(ds=ds, driver_serv_type=driver_serv_type, all_completed_num=all_completed_num)
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)
    return row_html

def get_trike_row(ds, driver_serv_type):
    tr_fmt = '''
       <tr>{row}</tr>
    '''
    row_fmt = '''
        <!--{}{}-->
        <td>{}</td>
        <td>{}</td>
        <!--关键指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--供需关系-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--司机指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--体验指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--乘客指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>
        <!--财务指标-->
        <td>{}</td>
        <td>{}</td>
        <td>{}</td>

    '''
    sql = '''
        with all_data as (
            SELECT
                dt,
                sum(request_num) as request_num,
                AVG(request_num_lfw) as request_num_lfw,
                SUM(completed_num) as completed_num,
                AVG(completed_num_lfw) as completed_num_lfw,
                SUM(completed_drivers) as completed_drivers,
                SUM(online_time_total) as online_time_total,
                SUM(online_drivers) as online_drivers,
                SUM(billing_time_total) as billing_time_total,
                SUM(push_num) as push_num,
                SUM(push_drivers) as push_drivers,
                SUM(register_drivers) as register_drivers,
                SUM(take_time_total) as take_time_total,
                SUM(pickup_time_total) as pickup_time_total,
                SUM(distance_total) as distance_total,
                SUM(request_usernum) as request_usernum,
                SUM(completed_users) as completed_users,
                SUM(first_completed_users) as first_completed_users,
                SUM(trike_complete_passengernum) as trike_complete_passengernum,
                SUM(trike_online_pay_passengernum) as trike_online_pay_passengernum,
                SUM(trike_pay_passengernum) as trike_pay_passengernum,
                SUM(new_user_completed_num) as new_user_completed_num,
                SUM(new_user_gmv) as new_user_gmv,
                SUM(gmv) as gmv,
                SUM(pay_num) as pay_num,
                SUM(pay_price) as pay_price,
                SUM(pay_amount) as pay_amount,
                SUM(b_subsidy) as b_subsidy,
                SUM(completed_driver_online_time_total) as completed_driver_online_time_total,
                SUM(finished_num) as finished_num,
                SUM(order_score_num) as order_score_num
            FROM
                oride_bi.oride_global_city_serv_daily_report
            WHERE
                dt='{ds}' AND driver_serv_type={driver_serv_type}
            GROUP BY
                dt
        ), city_data as (
            SELECT
                *
            FROM
                oride_bi.oride_global_city_serv_daily_report
            WHERE
                dt='{ds}' AND driver_serv_type={driver_serv_type}
        )
        -- 全部城市数据
        SELECT
            '1' as order_by,
            0 as city_id,
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            'All' as name,
            '-',--request_num,
            '-',--request_num_lfw,
            completed_num,
            cast(nvl(completed_num_lfw, 0) as int),
            '-',--concat(cast(nvl(round(completed_num * 100/request_num,1),0) as string),'%'),
            '-',--concat(cast(nvl(round(completed_num_lfw * 100/request_num_lfw,1),0) as string),'%'),
            nvl(finished_num, '--'),
            completed_drivers,
            if(completed_driver_online_time_total is null, '-', round(completed_driver_online_time_total/3600/completed_drivers, 1)),
            concat(cast(nvl(round(billing_time_total/completed_driver_online_time_total*100, 1),0) as string), '%'),
            nvl(round(push_num/push_drivers, 1),0),
            register_drivers,
            online_drivers,
            nvl(round(completed_num/completed_drivers, 1),0),
            if(completed_driver_online_time_total is null, '-', round(completed_num/(completed_driver_online_time_total/3600), 1)) as tph,
            if(order_score_num is null, '-', concat(cast(nvl(round(order_score_num/completed_num*10000, 1),0) as string), '‱')),
            cast(nvl(round(take_time_total/completed_num),0) as int),
            cast(nvl(round(pickup_time_total/completed_num),0) as int),
            cast(nvl(round(billing_time_total/completed_num),0) as int),
            cast(nvl(round(distance_total/completed_num),0) as int),
            if(dt>='2019-08-06',nvl(round((trike_complete_passengernum)/(completed_num),1),0),'-') as trike_order_passenger_avg,
            '-',--if(dt>='2019-08-06',(request_usernum),'-') as request_usernum,
            if(dt>='2019-08-06',(first_completed_users),'-') as first_completed_users,
            if(dt>='2019-08-06',concat(cast(nvl(round((first_completed_users) * 100/(completed_num),2),0) as string),'%'),'-') as first_completed_rate,
            if(dt>='2019-08-06',(completed_users - first_completed_users),'-') as old_completed_users,
            if(dt>='2019-08-06', concat(cast(nvl(round(trike_online_pay_passengernum * 100 / trike_pay_passengernum,1),0) as string),'%'), '-') as trike_online_pay_rate,
            '-' AS new_user_request_num,
            nvl(new_user_completed_num,0),
            nvl(new_user_gmv,0),
            nvl(round(pay_price/pay_num),0),
            concat(cast(nvl(round(b_subsidy/gmv*100, 1),0) as string), '%'),
            concat(cast(nvl(round((pay_price-pay_amount)/gmv*100, 1),0) as string), '%')
        FROM
            all_data
        -- 分城市数据
        UNION
        SELECT
            '2' as order_by,
            td.id as city_id,
            from_unixtime(unix_timestamp(cd.dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            td.name as name,
            '-', --cd.request_num
            '-', --cd.request_num_lfw,
            cd.completed_num,
            cast(nvl(cd.completed_num_lfw, 0) as int),
            '-',--concat(cast(nvl(round(cd.completed_num * 100/cd.request_num,1),0) as string),'%'),
            '-',--concat(cast(nvl(round(cd.completed_num_lfw * 100/cd.request_num_lfw,1),0) as string),'%'),
            nvl(cd.finished_num, '--'),
            cd.completed_drivers,
            if(cd.completed_driver_online_time_total is null, '-', round(cd.completed_driver_online_time_total/3600/cd.completed_drivers, 1)),
            concat(cast(nvl(round(cd.billing_time_total/cd.completed_driver_online_time_total*100, 1),0) as string), '%'),
            nvl(round(cd.push_num/cd.push_drivers, 1),0),
            cd.register_drivers,
            cd.online_drivers,
            nvl(round(cd.completed_num/cd.completed_drivers, 1),0),
            if(cd.completed_driver_online_time_total is null, '-', round(cd.completed_num/(cd.completed_driver_online_time_total/3600), 1)) as tph,
            if(cd.order_score_num is null, '-', concat(cast(nvl(round(cd.order_score_num/cd.completed_num*10000, 1),0) as string), '‱')),
            cast(nvl(round(cd.take_time_total/cd.completed_num),0) as int),
            cast(nvl(round(cd.pickup_time_total/cd.completed_num),0) as int),
            cast(nvl(round(cd.billing_time_total/cd.completed_num),0) as int),
            cast(nvl(round(cd.distance_total/cd.completed_num),0) as int),
            if(dt>='2019-08-06',nvl(round((cd.trike_complete_passengernum)/(cd.completed_num),1),0),'-') as trike_order_passenger_avg,
            '-',--if(dt>='2019-08-06',(cd.request_usernum),'-') as request_usernum,
            if(dt>='2019-08-06',(cd.first_completed_users),'-') as first_completed_users,
            if(dt>='2019-08-06',concat(cast(nvl(round((cd.first_completed_users) * 100/(cd.completed_num),2),0) as string),'%'),'-') as first_completed_rate,
            if(dt>='2019-08-06',(cd.completed_users - cd.first_completed_users),'-') as old_completed_users,
            if(dt>='2019-08-06', concat(cast(nvl(round(cd.trike_online_pay_passengernum * 100 / cd.trike_pay_passengernum,1),0) as string),'%'), '-') as trike_online_pay_rate,
            '-' AS new_user_request_num,
            nvl(cd.new_user_completed_num,0),
            nvl(cd.new_user_gmv,0),
            nvl(round(cd.pay_price/cd.pay_num),0),
            concat(cast(nvl(round(cd.b_subsidy/cd.gmv*100, 1),0) as string), '%'),
            concat(cast(nvl(round((cd.pay_price-cd.pay_amount)/cd.gmv*100, 1),0) as string), '%')
        FROM
            city_data cd
            INNER JOIN
            (
                SELECT
                    id,
                    name
                FROM
                    oride_dw_ods.ods_sqoop_base_data_city_conf_df
                WHERE
                   dt='{ds}'
            ) td ON td.id=cd.city_id
        ORDER BY order_by ASC, city_id ASC
    '''.format(ds=ds, driver_serv_type=driver_serv_type)
    cursor = get_hive_cursor()
    logging.info('Executing: %s', sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    row_html = ''
    if len(data_list) > 0:
        for data in data_list:
            row = row_fmt.format(*list(data))
            row_html += tr_fmt.format(row=row)
    return row_html

def send_report_email(ds, **kwargs):
    logging.info("receivers:%s" % Variable.get("oride_global_daily_report_receivers"))
    sql = '''
        SELECT
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') as week,
            request_num,
            request_num_lfw,
            completed_num,
            completed_num_lfw,
            nvl(round(completed_num/request_num*100, 1),0),
            nvl(round(completed_num_lfw/request_num_lfw*100, 1),0),
            active_users,
            completed_drivers,
            if(completed_driver_online_time_total is null, '-', round(completed_driver_online_time_total/3600/completed_drivers, 1)) as avg_online_time,
            if(billing_time is null or completed_driver_online_time_total is null, '-', concat(cast(nvl(round(billing_time/completed_driver_online_time_total * 100,1),0) as string),'%')) as btime_vs_otime,
            nvl(register_drivers, 0),
            nvl(online_drivers, ''),
            if(completed_drivers is null, '', nvl(round(completed_num/completed_drivers, 1),0)),
            avg_take_time,
            if(dt>='2019-07-02',avg_distance,'-') avg_distance,
            avg_pickup_time,
            register_users,
            first_completed_users,
            nvl(round(first_completed_users/completed_users*100, 1),0),
            completed_users-first_completed_users,
            map_request_num,


            if(dt>='2019-07-05', beckoning_num, '-') as beckoning_num,
            if(dt>='2019-07-05', driect_ordernum, '-') as driect_ordernum,
            if(dt>='2019-07-05', if(completed_num is null or completed_num=0, 0, concat(cast(round(driect_ordernum/completed_num*100,1) as string), '%')), '-') as driect_orderate,
            if(dt>='2019-07-05', driect_drivernum, '-') as driect_drivernum,
            if(dt>='2019-07-05', street_ordernum, '-') as street_ordernum,
            if(dt>='2019-07-05', if(completed_num is null or completed_num=0, 0, concat(cast(round(street_ordernum/completed_num*100,1) as string), '%')), '-') as street_orderate,
            if(dt>='2019-07-05', street_drivernum, '-') as street_drivernum,
            if(dt>='2019-07-05', request_usernum, '-') as request_usernum,
            if(dt>='2019-08-06', concat(cast(nvl(round(trike_online_pay_passengernum * 100 / trike_pay_passengernum,1),0) as string),'%'), '-') as trike_online_pay_rate,
            nvl(new_user_request_num, '-') AS new_user_request_num,
            nvl(new_user_completed_num, '-') AS new_user_completed_num,
            nvl(new_user_gmv, '-') AS new_user_gmv,
            if(completed_driver_online_time_total is null, '-', round(completed_num/(completed_driver_online_time_total/3600), 1)) as tph,
            nvl(finished_num, '-') as finished_num,
            if(dt>='2019-08-27',concat(cast(nvl(round(opay_pay_filed_order_num * 100 / opay_pay_order_num,1),0) as string),'%'),'-') as opay_papy_failed_rate,
            if(billing_time is null or completed_num is null, '-', nvl(round(billing_time/completed_num),0)) as avg_billtime
        FROM
           oride_bi.oride_global_daily_report
        WHERE
            dt <= '{dt}'
        ORDER BY dt DESC
        LIMIT 14
    '''.format(dt=ds)
    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    cursor.close()
    if len(data_list) > 0:
        html_fmt = '''
        <html>
        <head>
        <title></title>
        <style type="text/css">
            table
            {{
                font-family: "Trebuchet MS", Arial, Helvetica, sans-serif;
                border-collapse: collapse;
                margin: 0 auto;
                text-align: left;
                align:left;
            }}
            table td, table th
            {{
                border: 1px solid #000000;
                color: #000000;
                height: 30px;
                padding: 5px 10px 5px 5px;
            }}
            table thead th
            {{
                background-color: #f9cb9c;
                //color: white;
                width: 100px;
                color: #000000;
            }}
        </style>
        </head>
        <body>
            <table width="100%" class="table">
                <caption>
                    <h3>全部城市</h3>
                </caption>
            </table>
            <table width="100%" class="table">
                <thead>
                    <tr>
                        <th></th>
                        <th colspan="8" style="text-align: center;">关键指标</th>
                        <th colspan="4" style="text-align: center;">供需关系</th>
                        <th colspan="4" style="text-align: center;">司机指标</th>
                        <th colspan="4" style="text-align: center;">体验指标</th>
                        <th colspan="9" style="text-align: center;">乘客指标</th>
                        <th colspan="1" style="text-align: center;">财务</th>
                        <th colspan="1" style="text-align: center;">系统</th>
                        
                    </tr>
                    <tr>
                        <th>日期</th>
                        <!--关键指标-->
                        <th>下单数</th>
                        <th>下单数（近四周均值）</th>
                        <th>完单数</th>
                        <th>完单数（近四周均值）</th>
                        <th>完单率</th>
                        <th>完单率（近四周均值）</th>
                        <th>支付完单数</th>
                        <th>招手停完单数</th>
                        <!--供需关系-->
                        <th>活跃乘客数</th>
                        <th>完单司机数</th>
                        <th>人均在线时长（时）</th>
                        <th>计费时长占比</th>
                        <!--司机指标-->
                        <th>审核通过司机数</th>
                        <th>在线司机数</th>
                        <th>人均完单数</th>
                        <th>TPH</th>
                        <!--体验指标-->
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <th>平均计费时长（秒）</th>
                        <th>平均送驾距离（米）</th>
                        <!--乘客指标-->
                        <th>注册乘客数</th>
                        <th>下单乘客数</th>
                        <th>首次完单乘客数</th>
                        <th>完单新客占比</th>
                        <th>完单老乘客数</th>
                        <th>线上支付乘客占比</th>
                        <th>新用户下单数</th>
                        <th>新用户完单数</th>
                        <th>新用户GMV</th>
                        <!--财务-->
                        <th>地图调用次数</th>
                        <!--系统-->
                        <th>opay支付失败订单占比</th>
                    </tr>
                </thead>
                <tbody>
                {rows}
                </tbody>
            </table>
            <table width="100%" class="table">
                <caption>
                    <h3>多业务城市汇总</h3>
                </caption>
            </table>
            <table width="100%" class="table">
                <thead>
                    <tr>
                        <th></th>
                        <th></th>
                        <th colspan="2" style="text-align: center;">天气指标</th>
                        <th colspan="9" style="text-align: center;">关键指标</th>
                        <th colspan="4" style="text-align: center;">乘客指标</th>
                    </tr>
                    <tr>
                        <th>日期</th>
                        <th>城市</th>
                        <!--天气指标-->
                        <th>天气</th>
                        <th>湿单占比</th>
                        <!--关键指标-->
                        <th>下单数</th>
                        <th>下单数（近4周均值）</th>
                        <th>有效下单数</th>
                        <th>支付完单数</th>
                        <th>完单数</th>
                        <th>完单数（近4周均值）</th>
                        <th>完单率</th>
                        <th>完单率（近4周均值）</th>
                        <th>城市完单占比</th>
                        <!--乘客指标-->
                        <th>活跃乘客数</th>
                        <th>下单乘客数</th>
                        <th>完单乘客数</th>
                        <th>线上支付乘客占比</th>
                    </tr>
                </thead>
                <tbody>
                {city_rows}
                </tbody>
            </table>
            <table width="100%" class="table">
                <caption>
                    <h3>专车指标</h3>
                </caption>
            </table>
            <table width="100%" class="table">
                <thead>
                    <tr>
                        <th></th>
                        <th></th>
                        <th colspan="5" style="text-align: center;">关键指标</th>
                        <th colspan="5" style="text-align: center;">供需关系</th>
                        <th colspan="5" style="text-align: center;">司机指标</th>
                        <th colspan="4" style="text-align: center;">体验指标</th>
                        <th colspan="2" style="text-align: center;">乘客指标</th>
                        <th colspan="3" style="text-align: center;">财务指标</th>
                    </tr>
                    <tr>
                        <th>日期</th>
                        <th>城市</th>
                        <!--关键指标-->
                        <th>完单数</th>
                        <th>完单数（近四周均值）</th>
                        <th>支付完单数</th>
                        <th>专快完单占比</th>
                        <th>城市完单占比</th>
                        <!--供需关系-->
                        <th>完单用户数</th>
                        <th>完单司机数</th>
                        <th>人均在线时长（时）</th>
                        <th>计费时长占比</th>
                        <th>人均推送订单数</th>
                        <!--司机指标-->
                        <th>审核通过司机数</th>
                        <th>在线司机数</th>
                        <th>人均完单数</th>
                        <th>TPH</th>
                        <th>万单差评率</th>
                        <!--体验指标-->
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <th>平均计费时长（秒）</th>
                        <th>平均送驾距离（米）</th>
                        <!--乘客指标-->
                        <th>新用户完单数</th>
                        <th>新用户GMV</th>
                        <!--财务指标-->
                        <th>单均应付</th>
                        <th>B端补贴率</th>
                        <th>C端补贴率</th>
                    </tr>
                </thead>
                <tbody>
                {direct_rows}
                </tbody>
            </table>
            <table width="100%" class="table">
                <caption>
                    <h3>快车指标</h3>
                </caption>
            </table>
            <table width="100%" class="table">
                <thead>
                    <tr>
                        <th></th>
                        <th></th>
                        <th colspan="5" style="text-align: center;">关键指标</th>
                        <th colspan="5" style="text-align: center;">供需关系</th>
                        <th colspan="5" style="text-align: center;">司机指标</th>
                        <th colspan="4" style="text-align: center;">体验指标</th>
                        <th colspan="2" style="text-align: center;">乘客指标</th>
                        <th colspan="3" style="text-align: center;">财务指标</th>
                    </tr>
                    <tr>
                        <th>日期</th>
                        <th>城市</th>
                        <!--关键指标-->
                        <th>完单数</th>
                        <th>完单数（近四周均值）</th>
                        <th>支付完单数</th>
                        <th>专快完单占比</th>
                        <th>城市完单占比</th>
                        <!--供需关系-->
                        <th>完单用户数</th>
                        <th>完单司机数</th>
                        <th>人均在线时长（时）</th>
                        <th>计费时长占比</th>
                        <th>人均推送订单数</th>
                        <!--司机指标-->
                        <th>审核通过司机数</th>
                        <th>在线司机数</th>
                        <th>人均完单数</th>
                        <th>TPH</th>
                        <th>万单差评率</th>
                        <!--体验指标-->
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <th>平均计费时长（秒）</th>
                        <th>平均送驾距离（米）</th>
                        <!--乘客指标-->
                        <th>新用户完单数</th>
                        <th>新用户GMV</th>
                        <!--财务指标-->
                        <th>单均应付</th>
                        <th>B端补贴率</th>
                        <th>C端补贴率</th>
                    </tr>
                </thead>
                <tbody>
                {street_rows}
                </tbody>
            </table>

            <table width="100%" class="table">
                <caption>
                    <h3>OTrike指标</h3>
                </caption>
            </table>
            <table width="100%" class="table">
                <thead>
                    <tr>
                        <th></th>
                        <th></th>
                        <th colspan="7" style="text-align: center;">关键指标</th>
                        <th colspan="4" style="text-align: center;">供需关系</th>
                        <th colspan="5" style="text-align: center;">司机指标</th>
                        <th colspan="4" style="text-align: center;">体验指标</th>
                        <th colspan="9" style="text-align: center;">乘客指标</th>
                        <th colspan="3" style="text-align: center;">财务指标</th>
                    </tr>
                    <tr>
                        <th>日期</th>
                        <th>城市</th>
                        <!--关键指标-->
                        <th>下单数</th>
                        <th>下单数（近四周均值）</th>
                        <th>完单数</th>
                        <th>完单数（近四周均值）</th>
                        <th>完单率</th>
                        <th>完单率（近四周均值）</th>
                        <th>支付完单数</th>
                        <!--供需关系-->
                        <th>完单司机数</th>
                        <th>人均在线时长（时）</th>
                        <th>计费时长占比</th>
                        <th>人均推送订单数</th>
                        <!--司机指标-->
                        <th>审核通过司机数</th>
                        <th>在线司机数</th>
                        <th>人均完单数</th>
                        <th>TPH</th>
                        <th>万单差评率</th>
                        <!--体验指标-->
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <th>平均计费时长（秒）</th>
                        <th>平均送驾距离（米）</th>
                        <!--乘客指标-->
                        <th>单均乘客数</th>
                        <th>下单乘客数</th>
                        <th>首次完单乘客数</th>
                        <th>完单新客占比</th>
                        <th>完单老乘客数</th>
                        <th>线上支付乘客占比</th>
                        <th>新用户下单数</th>
                        <th>新用户完单数</th>
                        <th>新用户GMV</th>
                        <!--财务指标-->
                        <th>单均应付</th>
                        <th>B端补贴率</th>
                        <th>C端补贴率</th>
                    </tr>
                </thead>
                <tbody>
                {trike_rows}
                </tbody>
            </table>

        </body>
        </html>
        '''

        weekday = {
            "1": "周一",
            "2": "周二",
            "3": "周三",
            "4": "周四",
            "5": "周五",
            "6": "周六",
            "7": "周日",
        }
        tr_fmt = '''
            <tr>{row}</tr>
        '''
        weekend_tr_fmt = '''
            <tr style="background:#fff2cc">{row}</tr>
        '''
        row_fmt = '''
                <td>{dt}</td>
                <!--关键指标-->
                <td>{request_num}</td>
                <td>{request_num_lfw}</td>
                <td style="background:#d9d9d9">{completed_num}</td>
                <td>{completed_num_lfw}</td>
                <td style="background:#d9d9d9">{c_vs_r}%</td>
                <td>{c_vs_r_lfw}%</td>
                <td>{finished_num}</td>
                <td>{beckoning_num}</td>
                <!--供需关系-->
                <td>{active_users}</td>
                <td style="background:#d9d9d9">{completed_drivers}</td>
                <td>{avg_online_time}</td>
                <td>{btime_vs_otime}</td>
                <!--司机指标-->
                <td>{register_drivers}</td>
                <td>{online_drivers}</td>
                <td>{c_vs_od}</td>
                <td>{tph}</td>
                <!--体验指标-->
                <td>{avg_take_time}</td>
                <td>{avg_pickup_time}</td>
                <td>{avg_billtime}</td>
                <td>{avg_distance}</td>
                <!--乘客指标-->
                <td>{register_users}</td>
                <td>{order_users}</td>
                <td>{first_completed_users}</td>
                <td>{fcu_vs_cu}%</td>
                <td>{old_completed_users}</td>
                <td>{trike_online_pay_rate}</td>
                <td>{new_user_request_num}</td>
                <td>{new_user_completed_num}</td>
                <td>{new_user_gmv}</td>
                <!--财务-->
                <td>{map_request_num}</td>
                <!--系统-->
                <td>{opay_papy_failed_rate}</td>
        '''
        row_html = ''
        # 所有完单数
        all_completed_num = data_list[0][4]
        for data in data_list:
            [dt, week, request_num, request_num_lfw, completed_num, completed_num_lfw, c_vs_r, c_vs_r_lfw, active_users,
             completed_drivers, avg_online_time, btime_vs_otime, register_drivers, online_drivers, c_vs_od,
             avg_take_time, avg_distance, avg_pickup_time, register_users, first_completed_users, fcu_vs_cu,
             old_completed_users, map_request_num, beckoning_num, driect_ordernum, driect_orderate, driect_drivernum,
             street_ordernum, street_orderate, street_drivernum, request_usernum, trike_online_pay_rate, new_user_request_num,
             new_user_completed_num,new_user_gmv,tph, finished_num,opay_papy_failed_rate, avg_billtime] = list(data)
            row = row_fmt.format(
                dt=dt,
                request_num=request_num,
                request_num_lfw=request_num_lfw,
                completed_num=completed_num,
                completed_num_lfw=completed_num_lfw,
                c_vs_r=c_vs_r,
                c_vs_r_lfw=c_vs_r_lfw,
                active_users=active_users,
                completed_drivers=completed_drivers,
                register_drivers=register_drivers,
                online_drivers=online_drivers,
                c_vs_od=c_vs_od,
                avg_take_time=avg_take_time,
                avg_pickup_time=avg_pickup_time,
                avg_distance=avg_distance,
                register_users=register_users,
                first_completed_users=first_completed_users,
                fcu_vs_cu=fcu_vs_cu,
                old_completed_users=old_completed_users,
                map_request_num=map_request_num,
                avg_online_time=avg_online_time,
                btime_vs_otime=btime_vs_otime,
                order_users=request_usernum,
                beckoning_num=beckoning_num,
                driect_ordernum=driect_ordernum,
                driect_orderate=driect_orderate,
                driect_drivernum=driect_drivernum,
                street_ordernum=street_ordernum,
                street_orderate=street_orderate,
                street_drivernum=street_drivernum,
                trike_online_pay_rate=trike_online_pay_rate,
                new_user_request_num=new_user_request_num,
                new_user_completed_num=new_user_completed_num,
                new_user_gmv=new_user_gmv,
                tph=tph,
                finished_num=finished_num,
                opay_papy_failed_rate=opay_papy_failed_rate,
                avg_billtime=avg_billtime
            )
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)

        html = html_fmt.format(rows=row_html, direct_rows=get_serv_row(ds, 1, all_completed_num),
                               street_rows=get_serv_row(ds, 2, all_completed_num), trike_rows=get_trike_row(ds, 3), city_rows=get_city_row(ds, all_completed_num))
        # send mail

        email_to = Variable.get("oride_global_daily_report_receivers").split()
        #email_to = ['zhenqian.zhang@opay-inc.com']
        result = is_alert(ds, global_table_names)
        if result:
            email_to = ['bigdata@opay-inc.com']
            # email_to = ['nan.li@opay-inc.com']
        email_subject = 'oride全局运营指标_{}'.format(ds)
        send_email(
            email_to
            , email_subject, html, mime_charset='utf-8')
    return


send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

def send_funnel_report_email(ds, **kwargs):
    sql = '''
        SELECT
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') as week,
            nvl(oride_cllick_request_event_counter,0),
            nvl(estimated_price_cllick_request_event_counter,0),
            concat(nvl(round(estimated_price_cllick_request_event_counter/oride_cllick_request_event_counter*100, 2),0),'%') as ep_vs_oc,
            concat(nvl(round(estimated_price_cllick_request_event_counter_lfw/oride_cllick_request_event_counter_lfw*100, 2),0),'%')as ep_vs_oc_lfw,
            concat(nvl(round(request_a_ride_click_c_new/estimated_price_cllick_request_event_counter*100, 2),0),'%') as rq_vs_ep,
            concat(nvl(round(request_a_ride_click_lfw_c_new/estimated_price_cllick_request_event_counter_lfw*100, 2),0),'%') as rq_vs_ep_lfw,
            nvl(request_num,0),
            nvl(request_num_lfw,0),
            nvl(round((request_num-push_num)/request_num*100, 2),0) as no_push_rate,
            nvl(round(before_take_cancel_num/request_num*100, 2),0) as before_take_cancel_rate,
            nvl(round(before_take_cancel_num_lfw/request_num_lfw*100, 2),0) as before_take_cancel_lfw_rate,
            nvl(round(take_num/request_num*100, 2),0) as take_rate,
            nvl(round(take_num_lfw/request_num_lfw*100, 2),0) as take_lfw_rate,
            nvl(round(after_take_cancel_num/request_num*100, 2),0) as after_take_cancel_rate,
            nvl(round(after_take_cancel_num_lfw/request_num_lfw*100, 2),0) as after_take_cancel_lfw_rate,
            nvl(round(driver_cancel_num/request_num*100, 2),0) as driver_cancel_rate,
            nvl(round(driver_cancel_num_lfw/request_num_lfw*100, 2),0) as driver_cancel_lfw_rate,
            nvl(completed_num,0),
            nvl(completed_num_lfw,0),
            nvl(round(completed_num/request_num*100, 2),0) as completed_rate,
            nvl(round(completed_num_lfw/request_num_lfw*100, 2),0) as completed_lfw_rate,
            nvl(pay_num,0),
            if (dt >= '2019-06-26',  nvl(round(pay_price_total/pay_num, 2),0), ''),
            if (dt >= '2019-06-26',  nvl(round(pay_amount_total/pay_num, 2),0), ''),
            nvl(request_usernum, '-')
        FROM
           oride_bi.oride_global_daily_report
        WHERE
            dt <= '{dt}'
        ORDER BY dt DESC
        LIMIT 14
    '''.format(dt=ds)

    sql_city = '''
        SELECT
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
            from_unixtime(unix_timestamp(dt, 'yyyy-MM-dd'),'u') as week,
            nvl(city,'-') ,
            nvl(weather,'-'),
            concat(cast(nvl(round(rain_order_num * 100/request_num),0) as string),'%') as rain_order_rate,
            nvl(oride_cllick_request_event_counter,0),
            nvl(estimated_price_cllick_request_event_counter,0),
            nvl(round(estimated_price_cllick_request_event_counter/oride_cllick_request_event_counter*100, 2),0) as ep_vs_oc,
            nvl(round(estimated_price_cllick_request_event_counter_lfw/oride_cllick_request_event_counter_lfw*100, 2),0) as ep_vs_oc_lfw,
            nvl(round(request_num/estimated_price_cllick_request_event_counter*100, 2),0) as rq_vs_ep,
            nvl(round(request_num_lfw/estimated_price_cllick_request_event_counter_lfw*100, 2),0) as rq_vs_ep_lfw,
            nvl(request_num,0),
            nvl(request_num_lfw,0),
            nvl(round((request_num-push_num)/request_num*100, 2),0) as no_push_rate,
            nvl(round(before_take_cancel_num/request_num*100, 2),0) as before_take_cancel_rate,
            nvl(round(before_take_cancel_num_lfw/request_num_lfw*100, 2),0) as before_take_cancel_lfw_rate,
            nvl(round(take_num/request_num*100, 2),0) as take_rate,
            nvl(round(take_num_lfw/request_num_lfw*100, 2),0) as take_lfw_rate,
            nvl(round(after_take_cancel_num/request_num*100, 2),0) as after_take_cancel_rate,
            nvl(round(after_take_cancel_num_lfw/request_num_lfw*100, 2),0) as after_take_cancel_lfw_rate,
            nvl(round(driver_cancel_num/request_num*100, 2),0) as driver_cancel_rate,
            nvl(round(driver_cancel_num_lfw/request_num_lfw*100, 2),0) as driver_cancel_lfw_rate,
            nvl(completed_num,0),
            nvl(completed_num_lfw,0),
            nvl(round(completed_num/request_num*100, 2),0) as completed_rate,
            nvl(nvl(round(completed_num_lfw/request_num_lfw*100, 2),0),0) as completed_lfw_rate,
            pay_num,
            if (dt >= '2019-06-26',  nvl(round(pay_price_total/pay_num, 2),0), ''),
            if (dt >= '2019-06-26',  nvl(round(pay_amount_total/pay_num, 2),0), ''),
            nvl(request_usernum, '-')
        FROM
           oride_bi.oride_order_city_daily_report
        WHERE
            dt = '{dt}'
        ORDER BY dt DESC
        LIMIT 14
    '''.format(dt=ds)

    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()

    logging.info(sql_city)
    cursor.execute(sql_city)
    data_city_list = cursor.fetchall()

    html_fmt = '''
            <html>
            <head>
            <title></title>
            <style type="text/css">
                table
                {{
                    font-family:'黑体';
                    border-collapse: collapse;
                    margin: 0 auto;
                    text-align: left;
                    font-size:12px;
                    color:#29303A;
                }}
                table h2
                {{
                    font-size:20px;
                    color:#000000;
                }}
                .th_title
                {{
                    font-size:16px;
                    color:#000000;
                    text-align: center;
                }}
                table td, table th
                {{
                    border: 1px solid #000000;
                    color: #000000;
                    height: 30px;
                    padding: 5px 10px 5px 5px;
                }}
                table thead th
                {{
                    background-color: #1DCF9F;
                    //color: white;
                    width: 100px;
                }}
            </style>
            </head>
            <body>
                <table width="95%" class="table">
                    <caption>
                        <h2>订单漏斗模型</h2>
                    </caption>
                </table>


                <table width="100%" class="table">
                    <caption>
                        <h3>全部</h3>
                    </caption>
                </table>
                <table width="95%" class="table">
                    <thead>
                        <tr>
                            <th></th>
                            <th colspan="6" class="th_title">呼叫前(限客户端版本号>4.4.405)</th>
                            <th colspan="12" class="th_title">呼叫-应答</th>
                            <th colspan="7" class="th_title">完单-支付</th>
                        </tr>
                        <tr>
                            <th>日期</th>
                            <!--呼叫前-->
                            <th>地址选择次数</th>
                            <th>估价次数</th>
                            <th>地址选择-估价转化率</th>
                            <th>地址选择-估价转化率（近4周同期均值）</th>
                            <th>估价-下单转化率</th>
                            <th>估价-下单转化率（近4周同期均值）</th>
                            <!--呼叫-应答-->
                            <th>下单乘客数</th>
                            <th>下单数</th>
                            <th>下单数（近4周同期均值）</th>
                            <th>未播率</th>
                            <th>应答前取消率</th>
                            
                            <th>应答率</th>
                            <th>应答后取消率</th>
                            <th>应答后取消率（近4周同期均值）</th>
                            <th>司机取消率</th>
                            <th>司机取消率（近4周同期均值）</th>
                            <!--完单-支付-->
                            <th>完单数</th>
                            <th>完单数（近4周同期均值）</th>
                            <th>完单率</th>
                            <th>完单率（近4周同期均值）</th>
                            <th>支付订单数</th>
                            <th>单均应付</th>
                            <th>单均实付</th>
                        </tr>
                    </thead>
                    {rows}
                </table>

                <table width="100%" class="table">
                    <caption>
                        <h3>分城市</h3>
                    </caption>
                </table>

                 <table width="95%" class="table">
                    <thead>
                        <tr>
                            <th></th>
                            <th></th>
                            <th colspan="2" class="th_title">天气指标</th>
                            <th colspan="12" class="th_title">呼叫-应答</th>
                            <th colspan="7" class="th_title">完单-支付</th>
                        </tr>
                        <tr>
                            <th>日期</th>
                            <th>城市</th>
                            <!--天气指标-->
                            <th>天气</th>
                            <th>湿单占比</th>
                            <!--呼叫-应答-->
                            <th>下单乘客数</th>
                            <th>下单数</th>
                            <th>下单数（近4周同期均值）</th>
                            <th>未播率</th>
                            <th>应答前取消率</th>
                            
                            <th>应答率</th>
                            
                            <th>应答后取消率</th>
                            <th>应答后取消率（近4周同期均值）</th>
                            <th>司机取消率</th>
                            <th>司机取消率（近4周同期均值）</th>
                            <!--完单-支付-->
                            <th>完单数</th>
                            <th>完单数（近4周同期均值）</th>
                            <th>完单率</th>
                            <th>完单率（近4周同期均值）</th>
                            <th>支付订单数</th>
                            <th>单均应付</th>
                            <th>单均实付</th>
                        </tr>
                    </thead>
                    {city_rows}
                </table>

            </body>
            </html>
            '''

    row_html = ''
    city_row_html = ''

    if len(data_list) > 0:

        tr_fmt = '''
            <tr style="background-color:#F5F5F5;">{row}</tr>
        '''
        weekend_tr_fmt = '''
            <tr style="background:#FFD8BF">{row}</tr>
        '''
        row_fmt = '''
                 <td>{0}</td>
                <!--呼叫前-->
                <td>{2}</td>
                <td>{3}</td>
                <td>{4}</td>
                <td>{5}</td>
                <td>{6}</td>
                <td>{7}</td>
                <!--呼叫-应答-->
                <td>{26}</td>
                <td>{8}</td>
                <td>{9}</td>
                <td>{10}%</td>
                <td>{11}%</td>
                
                <td>{13}%</td>
                <td>{15}%</td>
                <td>{16}%</td>
                <td>{17}%</td>
                <td>{18}%</td>
                <!--完单-支付-->
                <td>{19}</td>
                <td>{20}</td>
                <td>{21}%</td>
                <td>{22}%</td>
                <td>{23}</td>
                <td>{24}</td>
                <td>{25}</td>
        '''

        for data in data_list:
            row = row_fmt.format(*list(data))
            week = data[1]
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)

    if len(data_city_list) > 0:

        tr_fmt = '''
            <tr style="background-color:#F5F5F5;">{row}</tr>
        '''
        weekend_tr_fmt = '''
            <tr style="background:#FFD8BF">{row}</tr>
        '''
        row_fmt = '''

                <td>{0}</td>
                <td>{2}</td>
                <!--天气指标-->
                <td>{3}</td>
                <td>{4}</td>
                <!--呼叫-应答-->
                <td>{29}</td>
                <td>{11}</td>
                <td>{12}</td>
                <td>{13}%</td>
                <td>{14}%</td>
                
                <td>{16}%</td>
                
                <td>{18}%</td>
                <td>{19}%</td>
                <td>{20}%</td>
                <td>{21}%</td>
                <!--完单-支付-->
                <td>{22}</td>
                <td>{23}</td>
                <td>{24}%</td>
                <td>{25}%</td>
                <td>{26}</td>
                <td>{27}</td>
                <td>{28}</td>
        '''
        for data in data_city_list:
            row = row_fmt.format(*list(data))
            week = data[1]
            if week == '6' or week == '7':
                city_row_html += weekend_tr_fmt.format(row=row)
            else:
                city_row_html += tr_fmt.format(row=row)

    html = html_fmt.format(rows=row_html, city_rows=city_row_html)
    # send mail

    email_to = Variable.get("oride_funnel_report_receivers").split()
    result = is_alert(ds, funnel_report_table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']

    email_subject = 'oride订单漏斗模型_{}'.format(ds)
    send_email(
        email_to
        , email_subject, html, mime_charset='utf-8')
    cursor.close()
    return


send_funnel_report = PythonOperator(
    task_id='send_funnel_report',
    python_callable=send_funnel_report_email,
    provide_context=True,
    dag=dag
)

create_oride_anti_fraud_daily_report = HiveOperator(
    task_id='create_oride_anti_fraud_daily_report',
    hql="""
        CREATE TABLE IF NOT EXISTS `oride_anti_fraud_daily_report`(
            `rule_name` string,
            `behavior_id` int,
            `driver_register_num` int,
            `user_register_num` int,
            `driver_silence_num` int,
            `user_silence_num` int,
            `abnormal_order_num` int,
            `abnormal_driver_num` int,
            `revoked_order_num` int,
            `order_amount` DECIMAL(10,2)
        )
        PARTITIONED BY (
            `dt` string)
        STORED AS PARQUET;
        CREATE TABLE IF NOT EXISTS `orider_anti_fraud_daily_report_result`(
            `day` string comment '日期',
            `rule_name` string comment '规则名称',
            `behavior_id` int comment '策略ID',
            `user_regist_num` int comment '注册拦截乘客数',
            `user_regist_rate` decimal(10, 4) comment '注册拦截乘客占比',
            `user_regist_1dayring` decimal(10, 4) comment '注册拦截总人数日环比',
            `user_regist_7dayring` decimal(10, 4) comment '注册拦截总人数7日环比',
            `driver_silence_num` int comment '事件中拦截司机数',
            `user_silence_num` int comment '事件中拦截乘客数',
            `driver_silence_rate` decimal(10, 4) comment '事件中拦截司机占比',
            `user_silence_rate` decimal(10, 4) comment '事件中拦截乘客占比',
            `driver_silence_1dayring` decimal(10, 4) comment '事件中拦截司机数日环比',
            `user_silence_1dayring` decimal(10, 4) comment '事件中拦截乘客数日环比',
            `driver_silence_7dayring` decimal(10, 4) comment '事件中拦截司机数7日环比',
            `user_silence_7dayring` decimal(10, 4) comment '事件汇总拦截乘客数7日环比',
            `abnormal_driver_num` int comment '扣款司机数',
            `abnormal_driver_rate` decimal(10, 4) comment '扣款司机人数占比',
            `abnormal_driver_1dayring` decimal(10, 4) comment '扣款司机数日环比',
            `abnormal_driver_7dayring` decimal(10, 4) comment '扣款司机数7日环比',
            `abnormal_order_num` int comment '扣款订单数',
            `abnormal_order_rate` decimal(10, 4) comment '扣款订单数占比',
            `abnormal_order_1dayring` decimal(10, 4) comment '扣款订单日环比',
            `abnormal_order_7dayring` decimal(10, 4) comment '扣款订单7日环比',
            `order_amount` decimal(15, 2) comment '扣款金额',
            `order_amount_1dayring` decimal(10, 4) comment '扣款金额日环比',
            `order_amount_7dayring` decimal(10, 4) comment '扣款金币7日环比',
            `revoked_order_num` int comment '累计revoke量',
            `revoked_order_rate` decimal(10, 4) comment '累计revoke率' 
        ) 
        PARTITIONED BY (
            `dt` string
        )
        STORED AS PARQUET
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_anti_fraud_daily_report = HiveOperator(
    task_id='insert_oride_anti_fraud_daily_report',
    hql="""
        ALTER TABLE oride_anti_fraud_daily_report DROP IF EXISTS PARTITION (dt = '{{ ds }}');
        INSERT OVERWRITE TABLE oride_anti_fraud_daily_report PARTITION (dt = '{{ ds }}')
        SELECT
            afs.name,
            afs.behavior,
            rd.driver_register_num,
            rd.user_register_num,
            sd.driver_silence_num,
            sd.user_silence_num,
            ao.abnormal_order_num,
            ao.abnormal_driver_num,
            ao.revoked_order_num,
            ao.order_amount
        FROM
            (
              SELECT
                *
              FROM
                oride_dw_ods.ods_sqoop_base_data_anti_fraud_strategy_df
              WHERE
                dt='{{ ds }}'
            ) afs
                LEFT JOIN (
                    select
                        dt,rule_name, count(distinct driverid) as driver_register_num, count(distinct userid) user_register_num
                    from
                        oride_source.anti_fraud
                        lateral view explode(split(abnormalstrategy, ',')) addtable as rule_name
                    where
                        dt = '{{ ds }}'
                        and action = 'UserRegister'
                        and abnormalstrategy <> ''
                    group by dt,rule_name
                ) rd on rd.dt=afs.dt and rd.rule_name=afs.name
                LEFT JOIN (
                    select
                        dt,
                        behavior,
                        count(distinct if(action='SilenceDriver2' or action='SilenceDriver', driverid, null)) as driver_silence_num,
                        count(distinct if(action='SilenceUser', userid, null)) as user_silence_num
                    from
                        oride_source.anti_fraud
                        lateral view explode(split(behaviors, ',')) addtable as behavior
                    where
                        dt = '{{ ds }}'
                        and behaviors <> ''
                        and behaviors is not null
                        and action in ('SilenceDriver2', 'SilenceDriver', 'SilenceUser')
                    group by dt,behavior
                ) sd on sd.dt=afs.dt and sd.behavior=afs.behavior
                LEFT JOIN (
                    select
                        dt,
                        behavior,
                        count(distinct order_id) as abnormal_order_num,
                        count(distinct driver_id) as abnormal_driver_num,
                        count(distinct if(is_revoked=1, order_id, null)) as revoked_order_num,
                        sum(amount) as order_amount
                    from
                        oride_dw_ods.ods_sqoop_base_data_abnormal_order_df
                        lateral view explode(split(behavior_ids, ',')) addtable as behavior
                    where
                        dt = '{{ ds }}'
                        and from_unixtime(create_time, 'yyyy-MM-dd')=dt
                    group by dt,behavior
                ) ao on ao.dt=afs.dt and ao.behavior=afs.behavior
            where
                afs.dt='{{ ds }}' AND
                !(rd.driver_register_num is null
                and rd.user_register_num is null
                and sd.driver_silence_num is null
                and sd.user_silence_num is null
                and ao.abnormal_order_num is null
                and ao.abnormal_driver_num is null
                and ao.revoked_order_num is null
                and ao.order_amount  is null
                )
        """,
    schema='oride_bi',
    dag=dag)

insert_orider_anti_fraud_daily_report_result = HiveOperator(
    task_id='insert_orider_anti_fraud_daily_report_result',
    hql='''
        ALTER TABLE orider_anti_fraud_daily_report_result DROP IF EXISTS PARTITION (dt = '{dt}');
        WITH last_day_data AS (
            SELECT
                *
            FROM
                oride_bi.oride_anti_fraud_daily_report
            WHERE
                dt='{last_day}'

        ),
        7_day_data as (
            SELECT
                behavior_id,
                avg(user_register_num) as avg_user_register_num,
                avg(driver_silence_num) as avg_driver_silence_num,
                avg(user_silence_num) as avg_user_silence_num,
                avg(abnormal_driver_num) as avg_abnormal_driver_num,
                avg(abnormal_order_num) as avg_abnormal_order_num,
                avg(order_amount) as avg_order_amount
            FROM
                oride_bi.oride_anti_fraud_daily_report
            WHERE
                dt BETWEEN '{day_start_7}' AND '{dt}'
            GROUP BY
                behavior_id
        ),
        y_7_day_data as (
            SELECT
                behavior_id,
                avg(user_register_num) as avg_user_register_num,
                avg(driver_silence_num) as avg_driver_silence_num,
                avg(user_silence_num) as avg_user_silence_num,
                avg(abnormal_driver_num) as avg_abnormal_driver_num,
                avg(abnormal_order_num) as avg_abnormal_order_num,
                avg(order_amount) as avg_order_amount
            FROM
                oride_bi.oride_anti_fraud_daily_report
            WHERE
                dt BETWEEN '{y_7_day_start}' AND '{last_day}'
            GROUP BY
                behavior_id
        )
        INSERT OVERWRITE TABLE orider_anti_fraud_daily_report_result PARTITION (dt = '{dt}')
        SELECT
            td.dt,
            td.rule_name,
            td.behavior_id,
            nvl(td.user_register_num, 0),
            nvl(round(td.user_register_num/gdr.register_users, 4), 0),
            nvl(round((td.user_register_num/ldd.user_register_num-1),4), 0),
            nvl(round((7dd.avg_user_register_num/y7dd.avg_user_register_num-1),4), 0),
            nvl(td.driver_silence_num, 0),
            nvl(td.user_silence_num, 0),
            nvl(round(td.driver_silence_num/gdr.online_drivers, 4), 0),
            nvl(round(td.user_silence_num/gdr.active_users, 4), 0),
            nvl(round((td.driver_silence_num/ldd.driver_silence_num-1), 4), 0),
            nvl(round((td.user_silence_num/ldd.user_silence_num-1), 4), 0), 
            nvl(round((7dd.avg_driver_silence_num/y7dd.avg_driver_silence_num-1), 4), 0), 
            nvl(round((7dd.avg_user_silence_num/y7dd.avg_user_silence_num-1), 4), 0), 
            nvl(td.abnormal_driver_num, 0), 
            nvl(round(td.abnormal_driver_num/gdr.online_pay_driver_num, 4), 0), 
            nvl(round(td.abnormal_driver_num/ldd.abnormal_driver_num-1, 4), 0), 
            nvl(round(7dd.avg_abnormal_driver_num/y7dd.avg_abnormal_driver_num-1, 4), 0), 
            nvl(td.abnormal_order_num, 0), 
            nvl(round(td.abnormal_order_num/gdr.online_pay_order_num, 4), 0), 
            nvl(round(td.abnormal_order_num/ldd.abnormal_order_num-1, 4), 0),
            nvl(round(7dd.avg_abnormal_order_num/y7dd.avg_abnormal_order_num-1, 4), 0),
            nvl(td.order_amount, 0), 
            nvl(round(td.order_amount/ldd.order_amount-1, 4), 0), 
            nvl(round(7dd.avg_order_amount/y7dd.avg_order_amount-1, 4), 0),
            nvl(td.revoked_order_num, 0),
            nvl(round(td.revoked_order_num/td.abnormal_order_num, 4), 0)
        FROM
            oride_bi.oride_anti_fraud_daily_report td
            INNER JOIN oride_bi.oride_global_daily_report gdr ON gdr.dt=td.dt
            LEFT JOIN last_day_data ldd on ldd.behavior_id=td.behavior_id
            LEFT JOIN 7_day_data 7dd on 7dd.behavior_id=td.behavior_id
            LEFT JOIN y_7_day_data y7dd on y7dd.behavior_id=td.behavior_id
        WHERE
            td.dt='{dt}'
    '''.format(
        dt='{{ ds }}',
        last_day='{{ macros.ds_add(ds, -1) }}',
        day_start_7='{{ macros.ds_add(ds, -6) }}',
        y_7_day_start='{{ macros.ds_add(ds, -7) }}'
    ),
    schema='oride_bi',
    dag=dag
)


def send_anti_fraud_report_email(ds, **kwargs):
    html_mail_fmt = '''
        <html>
        <head>
        <title></title>
        <style type="text/css">
            table
            {{
                font-family:'黑体';
                border-collapse: collapse;
                margin: 0 auto;
                text-align: left;
                font-size:12px;
                color:#29303A;
            }}
            table h2
            {{
                font-size:20px;
                color:#000000;
            }}
            .th_title
            {{
                font-size:16px;
                color:#000000;
                text-align: center;
            }}
            table td, table th
            {{
                border: 1px solid #000000;
                color: #000000;
                height: 30px;
                padding: 5px 10px 5px 5px;
            }}
            table thead th
            {{
                background-color: #1DCF9F;
                //color: white;
                width: 100px;
            }}
        </style>
        </head>
        <body>
            <table width="95%" class="table">
                <caption>
                    <h2>反作弊报表</h2>
                </caption>
            </table>
            {html_content_fmt}
        </body>
        </html>
    '''
    # 注册策略
    sql = '''
        SELECT 
            day, rule_name, behavior_id, user_regist_num, user_regist_rate, user_regist_1dayring, user_regist_7dayring, dt
        FROM oride_bi.orider_anti_fraud_daily_report_result 
        WHERE dt between '{last_7_day}' AND '{day}' AND 
            behavior_id IN (9, 10, 11, 12, 13) 
    '''.format(
        day=ds,
        last_7_day=airflow.macros.ds_add(ds, -6)
    )
    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    if len(data_list) > 0:
        html_regist_fmt = '''
            <table width="95%" class="table">
                <thead>
                    <tr>
                        <th rowspan=3>日期</th>
                        <th colspan="{colspan}" class="th_title">注册策略</th>
                    </tr>
                    <tr>{rule_title}</tr>
                    <tr>{rule_head}</tr>
                </thead>
                {rows}
            </table>
        '''
        tr_fmt = '''
            <tr style="background-color:#F5F5F5;">{row}</tr>
        '''
        row_html = ''
        rule_head = {}
        html_rule_title = ''
        html_rule_head = ''
        curr_day = set()
        rule_set = set()
        for day, rule_name, behavior_id, user_regist_num, user_regist_rate, user_regist_1dayring, user_regist_7dayring, dt in data_list:
            if day not in curr_day:
                curr_day.add(day)
            if behavior_id not in rule_set:
                rule_set.add(behavior_id)
                rule_head['rule_title_' + str(behavior_id)] = '''
                    <th colspan="4">{rule_name}</th>
                '''.format(rule_name=rule_name)
                rule_head['rule_head_' + str(behavior_id)] = '''
                    <th>当日注册拦截乘客人数</th>
                    <th>当日注册拦截乘客占比</th>
                    <th>注册拦截乘客数日环比增量</th>
                    <th>注册拦截乘客数7日环比增量</th>
                '''
            rule_head['rule_data_' + day + str(behavior_id)] = '''
                <td>{user_regist_num}</td>
                <td>{user_regist_rate:.2%}</td>
                <td>{user_regist_1dayring:.2%}</td>
                <td>{user_regist_7dayring:.2%}</td>
            '''.format(
                user_regist_num=user_regist_num,
                user_regist_rate=user_regist_rate,
                user_regist_1dayring=user_regist_1dayring,
                user_regist_7dayring=user_regist_7dayring
            )

        head_complete = False
        curr_day = sorted(curr_day, reverse=True)
        rule_set = sorted(rule_set, reverse=False)
        logging.info(curr_day)
        logging.info(rule_set)
        for d in curr_day:
            row_temp = '<td>{day}</td>'.format(day=d)
            for r in rule_set:
                if not head_complete:
                    html_rule_title += rule_head.get('rule_title_' + str(r), '<th colspan="4">--</th>')
                    html_rule_head += rule_head.get('rule_head_' + str(r), '<th>-</th><th>-</th><th>-</th><th>-</th>')
                row_temp += rule_head.get('rule_data_' + d + str(r), '<td>-</td><td>-</td><td>-</td><td>-</td>')
            row_html += tr_fmt.format(row=row_temp)
            head_complete = True

        html_regist_fmt = html_regist_fmt.format(
            colspan=len(rule_set) * 4,
            rule_title=html_rule_title,
            rule_head=html_rule_head,
            rows=row_html
        )
    else:
        html_regist_fmt = ''

    cursor.close()

    # 事中策略
    sql = '''
        SELECT 
            day, rule_name, behavior_id, driver_silence_num, driver_silence_rate, driver_silence_1dayring, driver_silence_7dayring, 
            user_silence_num, user_silence_rate, user_silence_1dayring, user_silence_7dayring, dt
        FROM oride_bi.orider_anti_fraud_daily_report_result 
        WHERE dt between '{last_7_day}' AND '{day}' AND 
            behavior_id IN (6, 7, 8, 14, 15, 16, 17, 18, 19, 20, 21, 22, 25) 
    '''.format(
        day=ds,
        last_7_day=airflow.macros.ds_add(ds, -6)
    )
    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    if len(data_list) > 0:
        html_mid_fmt = '''
            <table width="95%" class="table">
                <thead>
                    <tr>
                        <th rowspan=3>日期</th>
                        <th colspan="{colspan}" class="th_title">事中策略</th>
                    </tr>
                    <tr>{rule_title}</tr>
                    <tr>{rule_head}</tr>
                </thead>
                {rows}
            </table>
        '''
        tr_fmt = '''
            <tr style="background-color:#F5F5F5;">{row}</tr>
        '''
        row_html = ''
        rule_head = {}
        html_rule_title = ''
        html_rule_head = ''
        curr_day = set()
        rule_set = set()
        for day, rule_name, behavior_id, driver_silence_num, driver_silence_rate, driver_silence_1dayring, driver_silence_7dayring, user_silence_num, user_silence_rate, user_silence_1dayring, user_silence_7dayring, dt in data_list:
            if day not in curr_day:
                curr_day.add(day)
            if behavior_id not in rule_set:
                rule_set.add(behavior_id)
                rule_head['rule_title_' + str(behavior_id)] = '''
                            <th colspan="4">{rule_name}</th>
                        '''.format(rule_name=rule_name)
                if rule_name.find('乘客') >= 0:
                    rule_head['rule_head_' + str(behavior_id)] = '''
                            <th>事中拦截乘客人数</th>
                            <th>事中拦截乘客人数占比</th>
                            <th>事中拦截乘客数日环比增量</th>
                            <th>事中拦截乘客数7日环比增量</th>
                    '''
                else:
                    rule_head['rule_head_' + str(behavior_id)] = '''
                            <th>事中拦截司机人数</th>
                            <th>事中拦截司机人数占比</th>
                            <th>事中拦截司机数日环比增量</th>
                            <th>事中拦截司机数7日环比增量</th>
                    '''
            if rule_name.find('乘客') >= 0:
                rule_head['rule_data_' + day + str(behavior_id)] = '''
                            <td>{num}</td>
                            <td>{rate:.2%}</td>
                            <td>{o1dayring:.2%}</td>
                            <td>{o7dayring:.2%}</td>
                        '''.format(
                    num=user_silence_num,
                    rate=user_silence_rate,
                    o1dayring=user_silence_1dayring,
                    o7dayring=user_silence_7dayring
                )
            else:
                rule_head['rule_data_' + day + str(behavior_id)] = '''
                        <td>{num}</td>
                        <td>{rate:.2%}</td>
                        <td>{o1dayring:.2%}</td>
                        <td>{o7dayring:.2%}</td>
                    '''.format(
                    num=driver_silence_num,
                    rate=driver_silence_rate,
                    o1dayring=driver_silence_1dayring,
                    o7dayring=driver_silence_7dayring
                )

        head_complete = False
        curr_day = sorted(curr_day, reverse=True)
        rule_set = sorted(rule_set, reverse=False)
        logging.info(curr_day)
        logging.info(rule_set)
        for d in curr_day:
            row_temp = '<td>{day}</td>'.format(day=d)
            for r in rule_set:
                if not head_complete:
                    html_rule_title += rule_head.get('rule_title_' + str(r), '<th colspan="4">--</th>')
                    html_rule_head += rule_head.get('rule_head_' + str(r), '<th>-</th><th>-</th><th>-</th><th>-</th>')
                row_temp += rule_head.get('rule_data_' + d + str(r), '<td>-</td><td>-</td><td>-</td><td>-</td>')
            row_html += tr_fmt.format(row=row_temp)
            head_complete = True

        html_mid_fmt = html_mid_fmt.format(
            colspan=len(rule_set) * 4,
            rule_title=html_rule_title,
            rule_head=html_rule_head,
            rows=row_html
        )
    else:
        html_mid_fmt = ''
    cursor.close()

    # 事后策略
    sql = '''
        SELECT 
            day, rule_name, behavior_id, abnormal_driver_num, abnormal_driver_rate, abnormal_driver_1dayring, abnormal_driver_7dayring, 
            abnormal_order_num, abnormal_order_rate, abnormal_order_1dayring, abnormal_order_7dayring, order_amount, order_amount_1dayring, 
            order_amount_7dayring, revoked_order_num, revoked_order_rate, dt
        FROM oride_bi.orider_anti_fraud_daily_report_result 
        WHERE dt between '{last_7_day}' AND '{day}' AND 
            behavior_id IN (1, 2, 3, 4, 5, 23, 24) 
    '''.format(
        day=ds,
        last_7_day=airflow.macros.ds_add(ds, -6)
    )
    cursor = get_hive_cursor()
    logging.info(sql)
    cursor.execute(sql)
    data_list = cursor.fetchall()
    if len(data_list) > 0:
        html_after_fmt = '''
                <table width="95%" class="table">
                    <thead>
                        <tr>
                            <th rowspan=3>日期</th>
                            <th colspan="{colspan}" class="th_title">事后策略</th>
                        </tr>
                        <tr>{rule_title}</tr>
                        <tr>{rule_head}</tr>
                    </thead>
                    {rows}
                </table>
            '''
        tr_fmt = '''
                <tr style="background-color:#F5F5F5;">{row}</tr>
            '''
        row_html = ''
        rule_head = {}
        html_rule_title = ''
        html_rule_head = ''
        curr_day = set()
        rule_set = set()
        for day, rule_name, behavior_id, abnormal_driver_num, abnormal_driver_rate, abnormal_driver_1dayring, abnormal_driver_7dayring, abnormal_order_num, abnormal_order_rate, abnormal_order_1dayring, abnormal_order_7dayring, order_amount, order_amount_1dayring, order_amount_7dayring, revoked_order_num, revoked_order_rate, dt in data_list:
            if day not in curr_day:
                curr_day.add(day)
            if behavior_id not in rule_set:
                rule_set.add(behavior_id)
                rule_head['rule_title_' + str(behavior_id)] = '''
                                <th colspan="13">{rule_name}</th>
                            '''.format(rule_name=rule_name)

                rule_head['rule_head_' + str(behavior_id)] = '''
                            <th>扣款司机人数</th>
                            <th>扣款司机人数占比</th>
                            <th>扣款司机人数日环比增量</th>
                            <th>扣款司机人数7日环比增量</th>
                            <th>扣款订单量</th>
                            <th>扣款订单占比</th>
                            <th>扣款订单量日环比增量</th>
                            <th>扣款订单量7日环比增量</th>
                            <th>扣款金额</th>
                            <th>扣款金额日环比增量</th>
                            <th>扣款金额7日环比增量</th>
                            <th>累计revoke量</th>
                            <th>累计revoke率</th>
                    '''

            rule_head['rule_data_' + day + str(behavior_id)] = '''
                            <td>{abnormal_driver_num}</td>
                            <td>{abnormal_driver_rate:.2%}</td>
                            <td>{abnormal_driver_1dayring:.2%}</td>
                            <td>{abnormal_driver_7dayring:.2%}</td>
                            <td>{abnormal_order_num}</td>
                            <td>{abnormal_order_rate:.2%}</td>
                            <td>{abnormal_order_1dayring:.2%}</td>
                            <td>{abnormal_order_7dayring:.2%}</td>
                            <td>{order_amount}</td>
                            <td>{order_amount_1dayring:.2%}</td>
                            <td>{order_amount_7dayring:.2%}</td>
                            <td>{revoked_order_num}</td>
                            <td>{revoked_order_rate:.2%}</td>
                        '''.format(
                abnormal_driver_num=abnormal_driver_num,
                abnormal_driver_rate=abnormal_driver_rate,
                abnormal_driver_1dayring=abnormal_driver_1dayring,
                abnormal_driver_7dayring=abnormal_driver_7dayring,
                abnormal_order_num=abnormal_order_num,
                abnormal_order_rate=abnormal_order_rate,
                abnormal_order_1dayring=abnormal_order_1dayring,
                abnormal_order_7dayring=abnormal_order_7dayring,
                order_amount=order_amount,
                order_amount_1dayring=order_amount_1dayring,
                order_amount_7dayring=order_amount_7dayring,
                revoked_order_num=revoked_order_num,
                revoked_order_rate=revoked_order_rate
            )

        head_complete = False
        curr_day = sorted(curr_day, reverse=True)
        rule_set = sorted(rule_set, reverse=False)
        logging.info(curr_day)
        logging.info(rule_set)
        for d in curr_day:
            row_temp = '<td>{day}</td>'.format(day=d)
            for r in rule_set:
                if not head_complete:
                    html_rule_title += rule_head.get('rule_title_' + str(r), '<th colspan="13">--</th>')
                    html_rule_head += rule_head.get('rule_head_' + str(r),
                                                    '<th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th>')
                row_temp += rule_head.get('rule_data_' + d + str(r),
                                          '<td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>')
            row_html += tr_fmt.format(row=row_temp)
            head_complete = True

        html_after_fmt = html_after_fmt.format(
            colspan=len(rule_set) * 13,
            rule_title=html_rule_title,
            rule_head=html_rule_head,
            rows=row_html
        )
    else:
        html_after_fmt = ''
    cursor.close()

    # send mail

    email_to = Variable.get("oride_anti_fraud_report_receivers").split()
    result = is_alert(ds, anti_fraud_table_names)
    if result:
        email_to = ['bigdata@opay-inc.com']

    if html_mid_fmt != '' or html_regist_fmt != '' or html_after_fmt != '':
        email_subject = 'oride反作弊报表_{}'.format(ds)
        send_email(
            email_to,
            email_subject,
            html_mail_fmt.format(html_content_fmt=html_regist_fmt + '<hr>' + html_mid_fmt + '<hr>' + html_after_fmt),
            mime_charset='utf-8')
    return


send_anti_fraud_report = PythonOperator(
    task_id='send_anti_fraud_report',
    python_callable=send_anti_fraud_report_email,
    provide_context=True,
    dag=dag
)



'''
检查global报表依赖数据是否导入成功
'''
for table_name in global_table_names:
    global_report_validate_task = HivePartitionSensor(
        task_id="global_report_validate_task_{}_{}".format(*table_name.split('.')),
        table=table_name,
        partition="dt='{{ds}}'",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    global_report_validate_task >> insert_oride_global_daily_report

'''
检查订单漏斗报表依赖数据是否导入成功
'''
for table_name in funnel_report_table_names:
    funnel_report_validate_task = HivePartitionSensor(
        task_id="funnel_report_validate_task_{}_{}".format(*table_name.split('.')),
        table=table_name,
        partition="dt='{{ds}}'",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    funnel_report_validate_task >> insert_oride_order_city_daily_report

'''
检查反作弊报表依赖数据是否导入成功
'''
for table_name in anti_fraud_table_names:
    anti_fraud_validate_task = HivePartitionSensor(
        task_id="anti_fraud_validate_task_{}_{}".format(*table_name.split('.')),
        table=table_name,
        partition="dt='{{ds}}'",
        poke_interval=60,  # 依赖不满足时，一分钟检查一次依赖状态
        dag=dag
    )
    anti_fraud_validate_task >> insert_oride_anti_fraud_daily_report

create_oride_global_daily_report >> insert_oride_global_daily_report
import_opay_event_log >> insert_oride_global_daily_report
create_oride_global_city_serv_daily_report >> insert_oride_global_city_serv_daily_report
insert_oride_global_daily_report >> insert_oride_global_city_serv_daily_report
insert_oride_global_city_serv_daily_report >> send_report
insert_oride_order_city_daily_report >> send_report

create_oride_anti_fraud_daily_report >> insert_oride_anti_fraud_daily_report
insert_oride_global_daily_report >> insert_oride_anti_fraud_daily_report
insert_oride_anti_fraud_daily_report >> insert_orider_anti_fraud_daily_report_result >> send_anti_fraud_report

import_opay_event_log >> insert_oride_order_city_daily_report
insert_oride_order_city_daily_report >> send_funnel_report
insert_oride_global_daily_report >> send_funnel_report


