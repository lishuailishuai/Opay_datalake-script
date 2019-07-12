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

args = {
    'owner': 'root',
    'start_date': datetime(2019, 7, 10),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_global_daily_report',
    schedule_interval="30 00 * * *",
    default_args=args)


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

'''
modify by duo.wu at 2019-07-05 add 
    beckoning_num 招手停完单数，
    driect_ordernum 专车完单数，
    driect_drivernum 专车完单司机数，
    street_ordernum 快车完单数，
    street_drivernum 快车完单司机数,
    request_usernum 下单的乘客数
'''
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
            `request_usernum` int
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
                SUM(oride_cllick_request_event_counter) as oride_cllick_request_event_counter,
                SUM(estimated_price_cllick_request_event_counter) as estimated_price_cllick_request_event_counter
            FROM
                oride_source.appsflyer_opay_event_log
            WHERE
                dt='{{ ds }}'
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
                oride_db.data_order
            WHERE
                dt='{{ ds }}'
            GROUP BY dt
        ),
        -- 订单数据
        order_data as (
            SELECT
                do.dt,
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
                count(distinct do.user_id) as request_usernum
            FROM
                oride_db.data_order do
                LEFT JOIN
                (
                    SELECT
                        distinct user_id
                    FROM
                        oride_db.data_order
                    WHERE
                        dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd')<dt and status in (4,5)
                ) old_user on old_user.user_id=do.user_id
                LEFT JOIN oride_db.data_order_payment dop on dop.id=do.id and dop.dt=do.dt
            WHERE
                do.dt='{{ ds }}' and from_unixtime(do.create_time, 'yyyy-MM-dd')=do.dt
            GROUP BY do.dt
        ),
        -- 用户数据
        user_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as register_users,
                sum(if(from_unixtime(login_time, 'yyyy-MM-dd')=dt, 1, 0)) as active_users
            FROM
                oride_db.data_user_extend
            WHERE
                dt='{{ ds }}'
            GROUP BY dt
        ),
        -- 司机数据
        driver_data as (
            SELECT
                dt,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as register_drivers
            FROM
                oride_db.data_driver_extend
            WHERE
                dt='{{ ds }}'
            GROUP BY dt
        ),
        -- 司机在线数据
        online_data as (
            SELECT
                dt,
                count(1) as online_drivers,
                SUM(driver_onlinerange) as total_online_time
            FROM
                oride_bi.oride_driver_timerange
            WHERE
                dt='{{ ds }}'
            GROUP BY dt
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
            SELECT
                dt,
                count(distinct get_json_object(event_values, '$.order_id')) as push_num
            FROM
                oride_source.server_magic
            WHERE
                event_name='dispatch_push_driver' and dt='{{ ds }}'
            GROUP BY dt
        )
        INSERT OVERWRITE TABLE oride_global_daily_report PARTITION (dt = '{{ ds }}')
        SELECT
            od.request_num,
            lf.request_num_lfw,
            od.completed_num,
            lf.completed_num_lfw,
            od.completed_drivers,
            od.completed_users,
            od.first_completed_users,
            od.avg_take_time,
            od.avg_duration,
            od.avg_distance,
            old.online_drivers,
            old.total_online_time/old.online_drivers as avg_online_time,
            od.billing_time/old.total_online_time as btime_vs_otime,
            ud.active_users,
            ud.register_users,
            dd.register_drivers,
            nvl(md.map_request_num,0) as map_request_num,
            od.avg_pickup_time,
            ed.oride_cllick_request_event_counter,
            ed.estimated_price_cllick_request_event_counter,
            led.oride_cllick_request_event_counter_lfw,
            led.estimated_price_cllick_request_event_counter_lfw,
            lf.take_num_lfw,
            lf.before_take_cancel_num_lfw,
            lf.after_take_cancel_num_lfw,
            lf.driver_cancel_num_lfw,
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
            od.request_usernum 
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
        """,
    schema='oride_bi',
    dag=dag)

create_oride_global_city_daily_report = HiveOperator(
    task_id='create_oride_global_city_daily_report',
    hql="""
        CREATE TABLE IF NOT EXISTS `oride_global_city_daily_report`(
            `city_id` bigint COMMENT '所属城市ID',
            `request_num` int comment '下单数',
            `request_num_lfw` int comment '下单数（近4周均值）',
            `completed_num` int comment '完单数',
            `completed_num_lfw` int comment '完单数（近4周均值）',
            `beckoning_num` int comment '招手停完单数',
            `online_time_total` bigint comment '总在线时长',
            `online_drivers` int comment '总在线司机数',
            `billing_time_total` bigint comment '总计费时长',
            `register_drivers` int comment '注册司机数',
            `take_time_total` bigint comment '总应答时长（完单）',
            `pickup_time_total` bigint comment '总接驾时长（完单）',
            `distance_total` bigint comment '总送驾距离（完单）',
            `request_users` int comment '下单乘客数',
            `completed_users` int comment '完单乘客数',
            `first_completed_users` int comment '首次完单乘客数',
            `direct_completed_num` int comment '专车完单数',
            `direct_drivers` int comment '专车完单司机数',
            `street_completed_num` int comment '快车完单数',
            `street_drivers` int comment '快车完单司机数'
        )
        PARTITIONED BY (
            `dt` string)
        STORED AS PARQUET
        """,
    schema='oride_bi',
    dag=dag)

insert_oride_global_city_daily_report = HiveOperator(
    task_id='insert_oride_global_city_daily_report',
    hql="""
        ALTER TABLE oride_global_city_daily_report DROP IF EXISTS PARTITION (dt = '{{ ds }}');
        -- 近4周数据
        WITH lfw_data as (
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
                )/4 as completed_num_lfw
            FROM
                oride_db.data_order
            WHERE
                dt='{{ ds }}'
            GROUP BY dt,city_id
        ),
        -- 订单数据
        order_data as (
            SELECT
                do.dt,
                do.city_id,
                count(DISTINCT do.id) as request_num,
                count(DISTINCT if(do.status=4 or do.status=5, do.id, null)) as completed_num,
                count(distinct if(do.serv_type=99 and (do.status=4 or do.status=5), do.id, null)) as beckoning_num,
                SUM(if(do.arrive_time>0, do.arrive_time-do.pickup_time, 0)) as billing_time_total,
                SUM(if(do.status=4 or do.status=5, do.take_time-do.create_time, 0)) as take_time_total,
                SUM(if(do.status=4 or do.status=5, do.pickup_time-do.take_time, 0)) as pickup_time_total,
                sum(if(do.status = 4 or do.status =5,do.distance, 0)) as distance_total,
                count(distinct if(do.driver_serv_type=1 and (do.status=4 or do.status=5), do.id, null)) as direct_completed_num,
                count(distinct if(do.driver_serv_type=2 and (do.status=4 or do.status=5), do.id, null)) as street_completed_num,
                count(DISTINCT do.user_id) as request_users,
                count(DISTINCT if(do.status=4 or do.status=5, do.user_id, null)) as completed_users,
                count(DISTINCT if((do.status=4 or do.status=5) and old_user.user_id is null, do.user_id, null)) as first_completed_users
            FROM
                oride_db.data_order do
                LEFT JOIN
                (
                    SELECT
                        distinct user_id
                    FROM
                        oride_db.data_order
                    WHERE
                        dt='{{ ds }}' and from_unixtime(create_time, 'yyyy-MM-dd')<dt and status in (4,5)
                ) old_user on old_user.user_id=do.user_id
            WHERE
                do.dt='{{ ds }}' and from_unixtime(do.create_time, 'yyyy-MM-dd')=do.dt
            GROUP BY do.dt,do.city_id
        ),
        -- 司机数据
        driver_data as (
            SELECT
                dt,
                city_id,
                sum(if(from_unixtime(register_time, 'yyyy-MM-dd')=dt, 1, 0)) as register_drivers
            FROM
                oride_db.data_driver_extend
            WHERE
                dt='{{ ds }}'
            GROUP BY dt,city_id
        ),
        -- 司机在线数据
        online_data as (
            SELECT
                odt.dt,
                dde.city_id,
                count(distinct odt.driver_id) as online_drivers,
                SUM(odt.driver_onlinerange) as online_time_total
            FROM
                oride_bi.oride_driver_timerange odt
                INNER JOIN oride_db.data_driver_extend dde on dde.id=odt.driver_id and dde.dt=odt.dt
            WHERE
                odt.dt='{{ ds }}'
            GROUP BY odt.dt,dde.city_id
        ),
        -- 完单司机数据
        completed_driver_data as (
            SELECT
                do.dt,
                dde.city_id,
                count(distinct if(do.driver_serv_type=1 and (do.status=4 or do.status=5), do.driver_id, null)) as direct_drivers,
                count(distinct if(do.driver_serv_type=2 and (do.status=4 or do.status=5), do.driver_id, null)) as street_drivers
            FROM
                oride_db.data_order do
                INNER JOIN oride_db.data_driver_extend dde on dde.dt=do.dt and dde.id=do.driver_id
            WHERE
                do.dt='{{ds}}' AND from_unixtime(do.create_time, 'yyyy-MM-dd')=do.dt AND status in (4,5)
            GROUP BY
                do.dt, dde.city_id
        )
        INSERT OVERWRITE TABLE oride_global_city_daily_report PARTITION (dt = '{{ ds }}')
        SELECT
            od.city_id,
            od.request_num,
            ld.request_num_lfw,
            od.completed_num,
            ld.completed_num_lfw,
            od.beckoning_num,
            old.online_time_total,
            old.online_drivers,
            od.billing_time_total,
            dd.register_drivers,
            od.take_time_total,
            od.pickup_time_total,
            od.distance_total,
            od.request_users,
            od.completed_users,
            od.first_completed_users,
            od.direct_completed_num,
            cdd.direct_drivers,
            od.street_completed_num,
            cdd.street_drivers
        FROM
            order_data od
            LEFT JOIN lfw_data ld ON ld.dt=od.dt AND ld.city_id=od.city_id
            LEFT JOIN driver_data dd ON dd.dt=od.dt AND dd.city_id=od.city_id
            LEFT JOIN online_data old ON old.dt=od.dt AND old.city_id=od.city_id
            LEFT JOIN completed_driver_data cdd ON cdd.dt=od.dt AND cdd.city_id=od.city_id
        WHERE
            od.dt='{{ ds }}'
        """,
    schema='oride_bi',
    dag=dag)


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
            round(completed_num/request_num*100, 1),
            round(completed_num_lfw/request_num_lfw*100, 1),
            active_users,
            completed_drivers,
            if(dt>='2019-07-01', round(avg_online_time/3600,2), '-') as avg_online_time,
            if(dt>='2019-07-01', concat(cast(round(btime_vs_otime * 100,2) as string),'%'),'-') as btime_vs_otime,
            nvl(register_drivers, 0),
            nvl(online_drivers, ''),
            if(completed_drivers is null, '', round(completed_num/completed_drivers, 1)),
            avg_take_time,
            if(dt>='2019-07-02',avg_distance,'-') avg_distance,
            avg_pickup_time,
            register_users,
            first_completed_users,
            round(first_completed_users/completed_users*100, 1),
            completed_users-first_completed_users,
            map_request_num,
            if(dt>='2019-07-05', beckoning_num, '-') as beckoning_num,
            if(dt>='2019-07-05', driect_ordernum, '-') as driect_ordernum,
            if(dt>='2019-07-05', if(completed_num is null or completed_num=0, 0, concat(cast(round(driect_ordernum/completed_num*100,2) as string), '%')), '-') as driect_orderate,
            if(dt>='2019-07-05', driect_drivernum, '-') as driect_drivernum,
            if(dt>='2019-07-05', street_ordernum, '-') as street_ordernum,
            if(dt>='2019-07-05', if(completed_num is null or completed_num=0, 0, concat(cast(round(street_ordernum/completed_num*100,2) as string), '%')), '-') as street_orderate,
            if(dt>='2019-07-05', street_drivernum, '-') as street_drivernum,
            if(dt>='2019-07-05', request_usernum, '-') as request_usernum 
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
            }}
        </style>
        </head>
        <body>
            <table width="100%" class="table">
                <caption>
                    <h3>全部城市</h3>
                </caption>
                <thead>
                    <tr>
                        <th></th>
                        <th colspan="7" style="text-align: center;">关键指标</th>
                        <th colspan="4" style="text-align: center;">供需关系</th>
                        <th colspan="3" style="text-align: center;">司机指标</th>
                        <th colspan="3" style="text-align: center;">体验指标</th>
                        <th colspan="5" style="text-align: center;">乘客指标</th>
                        <th colspan="3" style="text-align: center;">专车指标</th>
                        <th colspan="3" style="text-align: center;">快车指标</th>
                        <th colspan="1" style="text-align: center;">财务</th>
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
                        <th>招手停完单数</th>
                        <!--供需关系-->
                        <th>活跃乘客数</th>
                        <th>完单司机数</th>
                        <th>人均在线时长（时）</th>
                        <th>计费时长占比</th>
                        <!--司机指标-->
                        <th>注册司机数</th>
                        <th>在线司机数</th>
                        <th>人均完单数</th>
                        <!--体验指标-->
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <th>平均送驾距离（米）</th>
                        <!--乘客指标-->
                        <th>注册乘客数</th>
                        <th>下单乘客数</th>
                        <th>首次完单乘客数</th>
                        <th>完单新客占比</th>
                        <th>完单老乘客数</th>
                        <!--专车指标-->
                        <th>完单数</th>
                        <th>完单占比</th>
                        <th>完单司机数</th>
                        <!--快车指标-->
                        <th>完单数</th>
                        <th>完单占比</th>
                        <th>完单司机数</th>
                        <!--财务-->
                        <th>地图调用次数</th>
                    </tr>
                </thead>
                {rows}
            </table>
            <table width="100%" class="table">
                <caption>
                    <h3>分城市指标</h3>
                </caption>
                <thead>
                    <tr>
                        <th></th>
                        <th></th>
                        <th colspan="8" style="text-align: center;">关键指标</th>
                        <th colspan="3" style="text-align: center;">供需关系</th>
                        <th colspan="3" style="text-align: center;">司机指标</th>
                        <th colspan="3" style="text-align: center;">体验指标</th>
                        <th colspan="4" style="text-align: center;">乘客指标</th>
                        <th colspan="3" style="text-align: center;">专车指标</th>
                        <th colspan="3" style="text-align: center;">快车指标</th>
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
                        <th>完单城市占比</th>
                        <th>招手停完单数</th>
                        <!--供需关系-->
                        <th>完单司机数</th>
                        <th>人均在线时长（时）</th>
                        <th>计费时长占比</th>
                        <!--司机指标-->
                        <th>注册司机数</th>
                        <th>在线司机数</th>
                        <th>人均完单数</th>
                        <!--体验指标-->
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <th>平均送驾距离（米）</th>
                        <!--乘客指标-->
                        <th>下单乘客数</th>
                        <th>首次完单乘客数</th>
                        <th>完单新客占比</th>
                        <th>完单老乘客数</th>
                        <!--专车指标-->
                        <th>完单数</th>
                        <th>完单占比</th>
                        <th>完单司机数</th>
                        <!--快车指标-->
                        <th>完单数</th>
                        <th>完单占比</th>
                        <th>完单司机数</th>
                    </tr>
                </thead>
                {city_rows}
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
                <th>{dt}</th>
                <!--关键指标-->
                <th>{request_num}</th>
                <th>{request_num_lfw}</th>
                <th style="background:#d9d9d9">{completed_num}</th>
                <th>{completed_num_lfw}</th>
                <th style="background:#d9d9d9">{c_vs_r}%</th>
                <th>{c_vs_r_lfw}%</th>
                <th>{beckoning_num}</th>
                <!--供需关系-->
                <th>{active_users}</th>
                <th style="background:#d9d9d9">{completed_drivers}</th>
                <th>{avg_online_time}</th>
                <th>{btime_vs_otime}</th>
                <!--司机指标-->
                <th>{register_drivers}</th>
                <th>{online_drivers}</th>
                <th>{c_vs_od}</th>
                <!--体验指标-->
                <th>{avg_take_time}</th>
                <th>{avg_pickup_time}</th>
                <th>{avg_distance}</th>
                <!--乘客指标-->
                <th>{register_users}</th>
                <th>{order_users}</th>
                <th>{first_completed_users}</th>
                <th>{fcu_vs_cu}%</th>
                <th>{old_completed_users}</th>
                <!--专车指标-->
                <th>{driect_ordernum}</th>
                <th>{driect_orderate}</th>
                <th>{driect_drivernum}</th>
                <!--快车指标-->
                <th>{street_ordernum}</th>
                <th>{street_orderate}</th>
                <th>{street_drivernum}</th>
                <!--财务-->
                <th>{map_request_num}</th>
        '''
        row_html = ''
        for data in data_list:
            [dt, week, request_num, request_num_lfw, completed_num, completed_num_lfw, c_vs_r, c_vs_r_lfw, active_users,
             completed_drivers, avg_online_time, btime_vs_otime, register_drivers, online_drivers, c_vs_od,
             avg_take_time, avg_distance, avg_pickup_time, register_users, first_completed_users, fcu_vs_cu,
             old_completed_users, map_request_num, beckoning_num, driect_ordernum, driect_orderate, driect_drivernum,
             street_ordernum, street_orderate, street_drivernum, request_usernum] = list(data)

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
                street_drivernum=street_drivernum
            )
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)



            city_sql = '''
                SELECT
                    from_unixtime(unix_timestamp(cd.dt, 'yyyy-MM-dd'),'yyyyMMdd') as dt,
                    nvl(dcc.name, cd.city_id),
                    nvl(cd.request_num,0),
                    if (cd.request_num_lfw is null or cd.request_num_lfw=0, '-', cd.request_num_lfw),
                    nvl(cd.completed_num,0),
                    if (cd.completed_num_lfw is null or cd.completed_num_lfw=0, '-', cd.completed_num_lfw),
                    round(cd.completed_num/cd.request_num*100, 1),
                    if (cd.request_num_lfw is null or cd.request_num_lfw=0, '-', concat(cast(round(cd.completed_num_lfw/cd.request_num_lfw*100, 1) as string), '%')),
                    nvl(cd.beckoning_num,0),
                    nvl(cd.direct_drivers + street_drivers,0),
                    nvl(round(cd.online_time_total/cd.online_drivers/3600, 2),0),
                    nvl(round(cd.billing_time_total/cd.online_time_total*100, 1),0),
                    nvl(cd.register_drivers,0),
                    nvl(cd.online_drivers,0),
                    nvl(round(cd.completed_num/(cd.direct_drivers + street_drivers), 1),0),
                    nvl(round(cd.take_time_total/cd.completed_num),0),
                    nvl(round(cd.pickup_time_total/cd.completed_num),0),
                    nvl(round(cd.distance_total/cd.completed_num),0),
                    nvl(cd.request_users,0),
                    nvl(cd.first_completed_users,0),
                    nvl(round(cd.first_completed_users/cd.completed_users*100, 1),0),
                    nvl(cd.completed_users-cd.first_completed_users,0),
                    nvl(cd.direct_completed_num,0),
                    nvl(round(cd.direct_completed_num/cd.completed_num*100, 1),0),
                    nvl(cd.direct_drivers,0),
                    nvl(cd.street_completed_num,0),
                    nvl(round(cd.street_completed_num/cd.completed_num*100, 1),0),
                    nvl(cd.street_drivers,0),
                    nvl(round(cd.completed_num/gdr.completed_num, 4),0)
                FROM
                   oride_bi.oride_global_city_daily_report cd
                   LEFT JOIN oride_db.data_city_conf dcc ON dcc.id=cd.city_id AND dcc.dt=cd.dt
                   INNER JOIN oride_bi.oride_global_daily_report gdr ON gdr.dt=cd.dt
                WHERE
                    cd.dt = '{dt}'
        '''.format(dt=ds)
        cursor = get_hive_cursor()
        logging.info(city_sql)
        cursor.execute(city_sql)
        city_data_list = cursor.fetchall()
        cursor.close()
        city_row_fmt = '''
            <td>{0}</td>
            <td>{1}</td>
            <!--关键指标-->
            <td>{2}</td>
            <td>{3}</td>
            <td style="background:#d9d9d9">{4}</td>
            <td>{5}</td>
            <td style="background:#d9d9d9">{6}%</td>
            <td>{7}</td>
            <td>{28:.2%}</td>
            <td>{8}</td>
            <!--供需关系-->
            <td style="background:#d9d9d9">{9}</td>
            <td>{10}</td>
            <td>{11}%</td>
            <!--司机指标-->
            <td>{12}</td>
            <td>{13}</td>
            <td>{14}</td>
            <!--体验指标-->
            <td>{15:.0f}</td>
            <td>{16:.0f}</td>
            <td>{17:.0f}</td>
            <!--乘客指标-->
            <td>{18}</td>
            <td>{19}</td>
            <td>{20}%</td>
            <td>{21}</td>
            <!--专车指标-->
            <td>{22}</td>
            <td>{23}%</td>
            <td>{24}</td>
            <!--快车指标-->
            <td>{25}</td>
            <td>{26}%</td>
            <td>{27}</td>
        '''
        city_row_html = ''
        if len(city_data_list) > 0:
            for data in city_data_list:
                row = city_row_fmt.format(*list(data))
                city_row_html += tr_fmt.format(row=row)
        html = html_fmt.format(rows=row_html, city_rows=city_row_html)
        # send mail
        email_subject = 'oride全局运营指标_{}'.format(ds)
        send_email(
            Variable.get("oride_global_daily_report_receivers").split()
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
            oride_cllick_request_event_counter,
            estimated_price_cllick_request_event_counter,
            round(estimated_price_cllick_request_event_counter/oride_cllick_request_event_counter*100, 2) as ep_vs_oc,
            round(estimated_price_cllick_request_event_counter_lfw/oride_cllick_request_event_counter_lfw*100, 2) as ep_vs_oc_lfw,
            round(request_num/estimated_price_cllick_request_event_counter*100, 2) as rq_vs_ep,
            round(request_num_lfw/estimated_price_cllick_request_event_counter_lfw*100, 2) as rq_vs_ep_lfw,
            request_num,
            request_num_lfw,
            round((request_num-push_num)/request_num*100, 2) as no_push_rate,
            round(before_take_cancel_num/request_num*100, 2) as before_take_cancel_rate,
            round(before_take_cancel_num_lfw/request_num_lfw*100, 2) as before_take_cancel_lfw_rate,
            round(take_num/request_num*100, 2) as take_rate,
            round(take_num_lfw/request_num_lfw*100, 2) as take_lfw_rate,
            round(after_take_cancel_num/request_num*100, 2) as after_take_cancel_rate,
            round(after_take_cancel_num_lfw/request_num_lfw*100, 2) as after_take_cancel_lfw_rate,
            round(driver_cancel_num/request_num*100, 2) as driver_cancel_rate,
            round(driver_cancel_num_lfw/request_num_lfw*100, 2) as driver_cancel_lfw_rate,
            completed_num,
            completed_num_lfw,
            round(completed_num/request_num*100, 2) as completed_rate,
            round(completed_num_lfw/request_num_lfw*100, 2) as completed_lfw_rate,
            pay_num,
            if (dt >= '2019-06-26',  round(pay_price_total/pay_num, 2), ''),
            if (dt >= '2019-06-26',  round(pay_amount_total/pay_num, 2), '')
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
    if len(data_list) > 0:
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
                <thead>
                    <tr>
                        <th></th>
                        <th colspan="6" class="th_title">呼叫前</th>
                        <th colspan="11" class="th_title">呼叫-应答</th>
                        <th colspan="7" class="th_title">完单-支付</th>
                    </tr>
                    <tr>
                        <th>日期</th>
                        <!--呼叫前-->
                        <th>地址选择需求数</th>
                        <th>估价需求数</th>
                        <th>地址选择-估价转化率</th>
                        <th>地址选择-估价转化率（近4周同期均值）</th>
                        <th>估价-下单转化率</th>
                        <th>估价-下单转化率（近4周同期均值）</th>
                        <!--呼叫-应答-->
                        <th>下单数</th>
                        <th>下单数（近4周同期均值）</th>
                        <th>未播率</th>
                        <th>应答前取消率</th>
                        <th>应答前取消率（近4周同期均值）</th>
                        <th>应答率</th>
                        <th>应答率（近4周同期均值）</th>
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
        </body>
        </html>
        '''
        tr_fmt = '''
            <tr style="background-color:#F5F5F5;">{row}</tr>
        '''
        weekend_tr_fmt = '''
            <tr style="background:#FFD8BF">{row}</tr>
        '''
        row_fmt = '''
                <td>{0}</td>
                <!--呼叫前-->
                <td><!--{2}--></td>
                <td><!--{3}--></td>
                <td><!--{4}%--></td>
                <td><!--{5}%--></td>
                <td><!--{6}%--></td>
                <td><!--{7}%--></td>
                <!--呼叫-应答-->
                <td>{8}</td>
                <td>{9}</td>
                <td>{10}%</td>
                <td>{11}%</td>
                <td>{12}%</td>
                <td>{13}%</td>
                <td>{14}%</td>
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
        row_html = ''
        for data in data_list:
            row = row_fmt.format(*list(data))
            week = data[1]
            if week == '6' or week == '7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)
        html = html_fmt.format(rows=row_html)
        # send mail
        email_subject = 'oride订单漏斗模型_{}'.format(ds)
        send_email(Variable.get("oride_funnel_report_receivers").split(), email_subject, html, mime_charset='utf-8')
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
            oride_db.data_anti_fraud_strategy afs
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
                        oride_db.data_abnormal_order
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
    #注册策略
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

    #事后策略
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
                    html_rule_head += rule_head.get('rule_head_' + str(r), '<th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th><th>-</th>')
                row_temp += rule_head.get('rule_data_' + d + str(r), '<td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td><td>-</td>')
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
    if html_mid_fmt != '' or html_regist_fmt != '' or html_after_fmt != '':
        email_subject = 'oride反作弊报表_{}'.format(ds)
        send_email(
            Variable.get("oride_anti_fraud_report_receivers").split(),
            email_subject,
            html_mail_fmt.format(html_content_fmt=html_regist_fmt+'<hr>'+html_mid_fmt + '<hr>' + html_after_fmt),
            mime_charset='utf-8')
    return


send_anti_fraud_report = PythonOperator(
    task_id='send_anti_fraud_report',
    python_callable=send_anti_fraud_report_email,
    provide_context=True,
    dag=dag
)

KeyDriverOnlineTime = "driver:ont:%d:%s"
KeyDriverOrderTime = "driver:ort:%d:%s"

get_driver_id = '''
select max(id) from oride_data.data_driver
'''
insert_timerange = '''
replace into bi.driver_timerange (`Daily`,`driver_id`,`driver_onlinerange`,`driver_freerange`) values (%s,%s,%s,%s)
'''


def get_driver_online_time(ds, **op_kwargs):
    dt = op_kwargs["ds_nodash"]
    redis = get_redis_connection()
    conn = get_db_conn('mysql_oride_data_readonly')
    mcursor = conn.cursor()
    mcursor.execute(get_driver_id)
    result = mcursor.fetchone()
    conn.commit()
    mcursor.close()
    conn.close()
    rows = []
    res = []
    for i in range(1, result[0] + 1):
        online_time = redis.get(KeyDriverOnlineTime % (i, dt))
        order_time = redis.get(KeyDriverOrderTime % (i, dt))
        if online_time is not None:
            if order_time is None:
                order_time = 0
            free_time = int(online_time) - int(order_time)
            res.append([dt + '000000', int(i), int(online_time), int(free_time)])
            rows.append('(' + str(i) + ',' + str(online_time, 'utf-8') + ',' + str(free_time) + ')')
    if rows:
        query = """
            INSERT OVERWRITE TABLE oride_bi.oride_driver_timerange PARTITION (dt='{dt}')
            VALUES {value}
        """.format(dt=ds, value=','.join(rows))
        logging.info('import_driver_online_time run sql:%s' % query)
        hive_hook = HiveCliHook()
        hive_hook.run_cli(query)
        # insert bi mysql
        conn = get_db_conn('mysql_bi')
        mcursor = conn.cursor()
        mcursor.executemany(insert_timerange, res)
        conn.commit()
        mcursor.close()
        conn.close()


import_driver_online_time = PythonOperator(
    task_id='import_driver_online_time',
    python_callable=get_driver_online_time,
    provide_context=True,
    dag=dag
)

create_oride_driver_timerange = HiveOperator(
    task_id='create_oride_driver_timerange',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_driver_timerange (
          driver_id int,
          driver_onlinerange int,
          driver_freerange int
        )
        PARTITIONED BY (
            dt STRING
        )
        STORED AS PARQUET
    """,
    schema='oride_bi',
    dag=dag)

create_oride_driver_timerange >> import_driver_online_time >> insert_oride_global_daily_report
insert_oride_global_daily_report >> insert_oride_anti_fraud_daily_report >> insert_orider_anti_fraud_daily_report_result >> send_anti_fraud_report
create_oride_anti_fraud_daily_report >> insert_oride_anti_fraud_daily_report
create_oride_global_daily_report >> insert_oride_global_daily_report
import_opay_event_log >> insert_oride_global_daily_report
insert_oride_global_daily_report >> send_funnel_report
create_oride_global_city_daily_report >> insert_oride_global_city_daily_report
insert_oride_global_city_daily_report >> send_report
insert_oride_global_daily_report >> insert_oride_global_city_daily_report
