# -*- coding: utf-8 -*-
"""
obus 汇总/分城市
"""
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.impala_plugin import ImpalaOperator
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn, get_db_conf
from utils.validate_metrics_utils import *
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.operators.bash_operator import BashOperator
import time
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 25),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_obus_report_path_detail_d',
    schedule_interval="00 09 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)

"""
依赖采集完成
"""
# 等待采集dag全部任务完成
dependence_ods_sqoop_conf_line_df = S3KeySensor(
    task_id='dependence_ods_sqoop_conf_line_df',
    bucket_key='obus_dw/ods_sqoop_conf_line_df/country_code=nal/dt={pt}/_SUCCESS'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_conf_station_df = S3KeySensor(
    task_id='dependence_ods_sqoop_conf_station_df',
    bucket_key='obus_dw/ods_sqoop_conf_station_df/country_code=nal/dt={pt}/_SUCCESS'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_conf_line_stations_df = S3KeySensor(
    task_id='dependence_ods_sqoop_conf_line_stations_df',
    bucket_key='obus_dw/ods_sqoop_conf_line_stations_df/country_code=nal/dt={pt}/_SUCCESS'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_data_driver_df = S3KeySensor(
    task_id='dependence_ods_sqoop_data_driver_df',
    bucket_key='obus_dw/ods_sqoop_data_driver_df/country_code=nal/dt={pt}/_SUCCESS'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_data_order_df = S3KeySensor(
    task_id='dependence_ods_sqoop_data_order_df',
    bucket_key='obus_dw/ods_sqoop_data_order_df/country_code=nal/dt={pt}/_SUCCESS'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_conf_city_df = S3KeySensor(
    task_id='dependence_ods_sqoop_conf_city_df',
    bucket_key='obus_dw/ods_sqoop_conf_city_df/country_code=nal/dt={pt}/_SUCCESS'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

"""
end
"""

sleep_time = BashOperator(
    task_id='sleep_id',
    depends_on_past=False,
    bash_command='sleep 120',
    dag=dag
)


def get_data_from_impala(**op_kwargs):
    ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time() - 86400)))
    sql = '''
        WITH
        --线路数据
        line_data as 
        (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                city_id,
                id,
                name                                    --line_name
            from obus_dw_ods.ods_sqoop_conf_line_df 
            where dt='{pt}'   
        ),
        --站点数据
        station_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                cs.city_id,
                cls.line_id,
                cs.id,                                  --站点ID
                cs.name                                 --站点名
            from (select 
                    id,
                    city_id,
                    name 
                from obus_dw_ods.ods_sqoop_conf_station_df 
                where dt='{pt}'
                ) as cs 
            left join (select 
                    line_id,
                    station_id 
                from obus_dw_ods.ods_sqoop_conf_line_stations_df 
                where dt='{pt}') as cls 
            on cs.id = cls.station_id
        ),
        --司机数据
        driver_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                cl.city_id,
                cl.id,
                cl.start_station,
                count(1) as total_drivers,                          --司机总人数
                sum(if(dd.serv_mode=1, 1, 0)) as serv_drivers,      --上班司机数
                sum(if(dd.serv_status=1 and dd.serv_mode=1, 1, 0)) as serv_on_the_road_drivers,     --上班行驶司机数
                sum(if(dd.serv_mode=1 and dd.serv_status in (0,2), 1, 0)) as serv_idle_drivers,     --上班未行驶司机数
                sum(if(dd.serv_mode=0, 1, 0)) as no_serv_drivers           --下班司机数
            from (select 
                    id,
                    city_id,
                    start_station
                from obus_dw_ods.ods_sqoop_conf_line_df 
                where dt='{pt}'
                ) as cl 
            join (select 
                    id,
                    line_id,
                    serv_mode,
                    serv_status
                from obus_dw_ods.ods_sqoop_data_driver_df 
                where dt='{pt}' and 
                    from_unixtime(login_time, 'yyyy-MM-dd')='{pt}'
                ) as dd 
            on cl.id = dd.line_id 
            group by cl.city_id, cl.id, cl.start_station
        ),
        --线路订单数据
        line_order_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                city_id,
                line_id,
                count(1) as lines_orders,                       ---线路总订单数
                sum(if(status in (1,2), 1, 0)) as lines_finished_orders,     ---线路总完单数
                sum(if(status in (1,2), price, 0)) as line_gmv_single       --线路收益（单）
            from obus_dw_ods.ods_sqoop_data_order_df 
            where dt='{pt}' and 
                from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}'
            group by city_id, line_id
        ),
        --站点订单数据
        station_order_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                city_id,
                line_id,
                start_station_id,
                count(1) as station_orders,                         --分站点订单数
                sum(if(status in (1,2), 1, 0)) as station_finished_orders,      ---分站点完单数
                count(distinct if(start_station_id>0 and status in (0,1,2), user_id, null)) as get_on_users --上车乘客数
            from obus_dw_ods.ods_sqoop_data_order_df 
            where dt='{pt}' and 
                from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' 
            group by city_id, line_id, start_station_id
        ),
        --新用户数量
        new_users_data as (
            select 
                 from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                 city_id,
                 line_id,
                 start_station_id,
                 sum(if(orders=1, 1, 0)) as new_users                       --新用户数量
            from (select 
                    city_id,
                    line_id,
                    start_station_id,
                    create_time,
                    user_id, 
                    row_number() over(partition by user_id order by arrive_time) orders
                from obus_dw_ods.ods_sqoop_data_order_df 
                where dt='{pt}' and 
                    status in (1,2) and 
                    user_id>0
                ) as t
            where from_unixtime(create_time,'yyyy-MM-dd')='{pt}' 
            group by city_id, line_id, start_station_id
        ),
        --下车乘客数
        get_off_users as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                city_id,
                line_id,
                end_station_id,
                count(distinct if(end_station_id>0 and status in (0,1,2), user_id, null)) as get_off_users --下车乘客数
            from obus_dw_ods.ods_sqoop_data_order_df 
            where dt='{pt}' and 
                from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' 
            group by city_id, line_id, end_station_id
        )

        --结果集
        select 
            station_data.dt,
            station_data.city_id,
            nvl(dc.name, ''),
            IF(station_data.line_id IS NULL, 0, station_data.line_id),
            nvl(line_data.name, ''),
            station_data.id,
            station_data.name,
            IF(driver_data.total_drivers IS NULL, 0, driver_data.total_drivers),
            IF(driver_data.serv_drivers IS NULL, 0, driver_data.serv_drivers),
            IF(driver_data.serv_on_the_road_drivers IS NULL, 0, driver_data.serv_on_the_road_drivers),
            IF(driver_data.serv_idle_drivers IS NULL, 0, driver_data.serv_idle_drivers),
            IF(driver_data.no_serv_drivers IS NULL, 0, driver_data.no_serv_drivers),
            IF(line_order_data.lines_orders IS NULL, 0, line_order_data.lines_orders),
            IF(station_order_data.station_orders IS NULL, 0, station_order_data.station_orders),
            IF(line_order_data.lines_finished_orders IS NULL, 0, line_order_data.lines_finished_orders),
            IF(station_order_data.station_finished_orders IS NULL, 0, station_order_data.station_finished_orders),
            IF(new_users_data.new_users IS NULL, 0, new_users_data.new_users),
            IF(station_order_data.get_on_users IS NULL, 0, station_order_data.get_on_users),
            IF(get_off_users.get_off_users IS NULL, 0, get_off_users.get_off_users),
            IF(line_order_data.line_gmv_single IS NULL, 0, line_order_data.line_gmv_single)
        from station_data 
        left join line_data on station_data.dt=line_data.dt and 
                                station_data.city_id=line_data.city_id and 
                                station_data.line_id=line_data.id 
        left join driver_data on station_data.dt = driver_data.dt and 
                                station_data.city_id = driver_data.city_id and 
                                station_data.line_id = driver_data.id and 
                                station_data.id = driver_data.start_station 
        left join line_order_data on station_data.dt = line_order_data.dt and 
                                station_data.city_id = line_order_data.city_id and 
                                station_data.line_id = line_order_data.line_id  
        left join station_order_data on station_data.dt = station_order_data.dt and 
                                station_data.city_id = station_order_data.city_id and 
                                station_data.line_id = station_order_data.line_id and 
                                station_data.id = station_order_data.start_station_id 
        left join new_users_data on station_data.dt = new_users_data.dt and 
                                station_data.city_id = new_users_data.city_id and 
                                station_data.line_id = new_users_data.line_id and 
                                station_data.id = new_users_data.start_station_id 
        left join get_off_users on station_data.dt = get_off_users.dt and 
                                station_data.city_id = get_off_users.city_id and 
                                station_data.line_id = get_off_users.line_id and 
                                station_data.id = get_off_users.end_station_id 
        left join (select id, name from obus_dw_ods.ods_sqoop_conf_city_df where dt='{pt}' and validate=1) as dc 
            on station_data.city_id = dc.id 

    '''.format(
        pt=ds
    )
    logging.info(sql)
    hive_cursor = get_hive_cursor()
    hive_cursor.execute(sql)
    result = hive_cursor.fetchall()

    mysql_conn = get_db_conn('mysql_bi')
    mcursor = mysql_conn.cursor()
    __data_to_mysql(mcursor, result,
                    ['dt', 'city_id', 'city', 'line_id', 'line_name', 'station_id',
                     'station_name', 'total_drivers', 'serv_drivers', 'serv_on_the_road_drivers',
                     'serv_idle_drivers', 'no_serv_drivers', 'lines_orders', 'station_orders', 'lines_finished_orders',
                     'station_finished_orders', 'new_users', 'get_on_users', 'get_off_users', 'line_gmv_single'],
                    '''
                        total_drivers=values(total_drivers),
                        serv_drivers=values(serv_drivers),
                        serv_on_the_road_drivers=values(serv_on_the_road_drivers),
                        serv_idle_drivers=values(serv_idle_drivers),
                        no_serv_drivers=values(no_serv_drivers),
                        lines_orders=values(lines_orders),
                        station_orders=values(station_orders),
                        lines_finished_orders=values(lines_finished_orders),
                        station_finished_orders=values(station_finished_orders),
                        new_users=values(new_users),
                        get_on_users=values(get_on_users),
                        get_off_users=values(get_off_users),
                        line_gmv_single=values(line_gmv_single)
                    '''
                    )

    hive_cursor.close()
    mcursor.close()


def __data_to_mysql(conn, data, column, update=''):
    isql = 'insert into obus_dw.app_obus_report_path_detail_d ({})'.format(','.join(column))
    esql = '{0} values {1} on duplicate key update {2}'
    sval = ''
    cnt = 0
    try:
        for (dt, city_id, name, line_id, lname, station_id, station_name, total_drivers,
             serv_drivers, serv_on_the_road_drivers, serv_idle_drivers, no_serv_drivers,
             lines_orders, station_orders, lines_finished_orders, station_finished_orders,
             new_users, get_on_users, get_off_users, line_gmv_single) in data:

            row = [dt, city_id, name, line_id, lname, station_id, station_name, total_drivers,
                   serv_drivers, serv_on_the_road_drivers, serv_idle_drivers, no_serv_drivers,
                   lines_orders, station_orders, lines_finished_orders, station_finished_orders,
                   new_users, get_on_users, get_off_users, line_gmv_single]
            if sval == '':
                sval = '(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            else:
                sval += ',(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            cnt += 1
            if cnt >= 1000:
                logging.info(esql.format(isql, sval, update))
                conn.execute(esql.format(isql, sval, update))
                cnt = 0
                sval = ''

        if cnt > 0 and sval != '':
            logging.info(esql.format(isql, sval, update))
            conn.execute(esql.format(isql, sval, update))
    except BaseException as e:
        logging.info(e)
        return


get_data_from_impala_task = PythonOperator(
    task_id='get_data_from_impala_task',
    python_callable=get_data_from_impala,
    provide_context=True,
    dag=dag
)

dependence_ods_sqoop_conf_line_df >> sleep_time
dependence_ods_sqoop_conf_station_df >> sleep_time
dependence_ods_sqoop_conf_line_stations_df >> sleep_time
dependence_ods_sqoop_data_driver_df >> sleep_time
dependence_ods_sqoop_data_order_df >> sleep_time
dependence_ods_sqoop_conf_city_df >> sleep_time

sleep_time >> get_data_from_impala_task
