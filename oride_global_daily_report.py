import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.email import send_email
from airflow.operators.hive_operator import HiveOperator
from utils.connection_helper import get_hive_cursor
import logging
from airflow.models import Variable

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 21),
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
            `avg_pickup_time` int
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
        ALTER TABLE oride_global_daily_report DROP IF EXISTS PARTITION (dt = '{{ ds }}');
        -- 近4周数据
        with lfw_data as (
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
                )/4 as completed_num_lfw
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
                avg(if(do.take_time>0, do.take_time-do.create_time, null)) as avg_take_time,
                avg(if(do.pickup_time>0, do.pickup_time-do.take_time, null)) as avg_pickup_time,
                avg(if(do.duration>0, do.duration, null)) as avg_duration,
                avg(if(do.distance>0, do.distance, null)) as avg_distance,
                SUM(do.duration) as total_duration,
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
                count(distinct user_id) as online_drivers,
                sum(if(period_s > 300, 0, period_s)) as total_online_time
            FROM
            (
                select
                    dt,
                    user_id,
                    `timestamp`-LAG(`timestamp`, 1, 0) OVER (PARTITION BY user_id ORDER BY `timestamp`) period_s
                from
                    oride_bi.oride_client_event_detail
                where
                    dt='{{ ds }}'
                    and event_name='active'
                    and get_json_object(event_value, '$.status')=1
                    and user_id>0
                    and app_name='ORide Driver'
            ) t
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
            od.total_duration/old.total_online_time as btime_vs_otime,
            ud.active_users,
            ud.register_users,
            dd.register_drivers,
            nvl(md.map_request_num,0) as map_request_num,
            od.avg_pickup_time
        FROM
            order_data od
            LEFT JOIN lfw_data lf on lf.dt=od.dt
            LEFT JOIN online_data old on old.dt=od.dt
            LEFT JOIN user_data ud on ud.dt=od.dt
            LEFT JOIN driver_data dd on dd.dt=od.dt
            LEFT JOIN map_data md on md.dt=od.dt
        """,
    schema='oride_bi',
    dag=dag)

def send_report_email(ds, **kwargs):
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
            0 as avg_online_time,
            0 as btime_vs_otime,
            nvl(register_drivers, 0),
            nvl(online_drivers, ''),
            if(online_drivers is null, '', round(completed_num/online_drivers, 1)),
            avg_take_time,
            avg_distance,
            avg_pickup_time,
            register_users,
            first_completed_users,
            round(first_completed_users/completed_users*100, 1),
            completed_users-first_completed_users,
            map_request_num
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
    if len(data_list) > 0 :
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
            <table width="95%" class="table">
                <caption>
                    <h2>全局运营指标</h2>
                </caption>
                <thead>
                    <tr>
                        <th></th>
                        <th colspan="6" style="text-align: center;">关键指标</th>
                        <th colspan="4" style="text-align: center;">供需关系</th>
                        <th colspan="3" style="text-align: center;">司机指标</th>
                        <th colspan="2" style="text-align: center;">体验指标</th>
                        <th colspan="4" style="text-align: center;">乘客指标</th>
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
                        <!--供需关系-->
                        <th>活跃乘客数</th>
                        <th>完单司机数</th>
                        <th>人均在线时长（秒）</th>
                        <th>计费时长占比</th>
                        <!--司机指标-->
                        <th>注册司机数</th>
                        <th>在线司机数</th>
                        <th>人均完单数</th>
                        <!--体验指标-->
                        <th>平均应答时长（秒）</th>
                        <th>平均接驾时长（秒）</th>
                        <!--乘客指标-->
                        <th>注册乘客数</th>
                        <th>首次完单乘客数</th>
                        <th>完单新客占比</th>
                        <th>完单老乘客数</th>
                        <!--财务-->
                        <th>地图调用次数</th>
                    </tr>
                </thead>
                {rows}
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
        row_fmt  = '''
                <th>{dt}</th>
                <!--关键指标-->
                <th>{request_num}</th>
                <th>{request_num_lfw}</th>
                <th style="background:#d9d9d9">{completed_num}</th>
                <th>{completed_num_lfw}</th>
                <th style="background:#d9d9d9">{c_vs_r}%</th>
                <th>{c_vs_r_lfw}%</th>
                <!--供需关系-->
                <th>{active_users}</th>
                <th style="background:#d9d9d9">{completed_drivers}</th>
                <th></th>
                <th></th>
                <!--司机指标-->
                <th>{register_drivers}</th>
                <th>{online_drivers}</th>
                <th>{c_vs_od}</th>
                <!--体验指标-->
                <th>{avg_take_time}</th>
                <th>{avg_pickup_time}</th>
                <!--乘客指标-->
                <th>{register_users}</th>
                <th>{first_completed_users}</th>
                <th>{fcu_vs_cu}%</th>
                <th>{old_completed_users}</th>
                <!--财务-->
                <th>{map_request_num}</th>
        '''
        row_html=''
        for data in data_list:
            [dt, week, request_num, request_num_lfw, completed_num, completed_num_lfw, c_vs_r, c_vs_r_lfw, active_users, completed_drivers, avg_online_time, btime_vs_otime, register_drivers, online_drivers, c_vs_od, avg_take_time, avg_distance, avg_pickup_time, register_users, first_completed_users, fcu_vs_cu, old_completed_users, map_request_num] = list(data)
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
                register_users=register_users,
                first_completed_users=first_completed_users,
                fcu_vs_cu=fcu_vs_cu,
                old_completed_users=old_completed_users,
                map_request_num=map_request_num
            )
            if week=='6' or week=='7':
                row_html += weekend_tr_fmt.format(row=row)
            else:
                row_html += tr_fmt.format(row=row)
        html = html_fmt.format(rows=row_html)
        # send mail
        email_subject = 'oride全局运营指标_{}'.format(ds)
        send_email(Variable.get("oride_global_daily_report_receivers").split(), email_subject, html, mime_charset='utf-8')
    cursor.close()
    return

send_report = PythonOperator(
    task_id='send_report',
    python_callable=send_report_email,
    provide_context=True,
    dag=dag
)

create_oride_global_daily_report >> insert_oride_global_daily_report
insert_oride_global_daily_report >> send_report