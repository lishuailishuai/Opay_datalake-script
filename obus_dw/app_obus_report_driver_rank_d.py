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
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
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
    'app_obus_report_driver_rank_d',
    schedule_interval="00 05 * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)

"""
依赖采集完成
"""
#等待采集dag全部任务完成
dependence_ods_sqoop_data_driver_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_data_driver_df',
    prefix='obus_dw/ods_sqoop_data_driver_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_data_driver_work_log_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_data_driver_work_log_df',
    prefix='obus_dw/ods_sqoop_data_driver_work_log_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_data_driver_trip_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_data_driver_trip_df',
    prefix='obus_dw/ods_sqoop_data_driver_trip_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_conf_line_stations_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_conf_line_stations_df',
    prefix='obus_dw/ods_sqoop_conf_line_stations_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_data_driver_records_day_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_data_driver_records_day_df',
    prefix='obus_dw/ods_sqoop_data_driver_records_day_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
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
        driver_data as 
        (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                city_id,
                id,
                real_name                                    --司机名字
            from obus_dw_ods.ods_sqoop_data_driver_df 
            where dt='{pt}' 
        ),
        --工作数据
        work_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                dr.city_id,
                dr.id as driver_id,
                sum(if(dw.serv_mode=1 and dw.serv_mode1=0, round(abs(dw.create_time2-dw.create_time)/3600,2), 0)) as work_dur             --司机今日在线时长(小时)
            from 
                (select 
                    driver_id,
                    serv_mode,
                    create_time,
                    lead(serv_mode,1,0) over(partition by driver_id order by create_time) serv_mode1,
                    lead(create_time,1,unix_timestamp('{pt} 23:59:59','yyyy-MM-dd HH:mm:ss')) over(partition by driver_id order by create_time) create_time2
                from obus_dw_ods.ods_sqoop_data_driver_work_log_df 
                where dt='{pt}' and 
                    from_unixtime(create_time, 'yyyy-MM-dd')='{pt}'
                ) as dw 
            join (select 
                    id, 
                    city_id 
                from obus_dw_ods.ods_sqoop_data_driver_df 
                where dt='{pt}'
                ) as dr 
            on dw.driver_id = dr.id 
            group by dr.city_id, dr.id
        ),
        --司机圈数
        driver_cycle_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                city_id,
                driver_id,
                count(distinct id)/2 as cycle_cnt                             --司机圈数
            from obus_dw_ods.ods_sqoop_data_driver_trip_df 
            where dt = '{pt}' and 
                from_unixtime(end_time, 'yyyy-MM-dd') = '{pt}' and 
                status = 1 
            group by city_id, driver_id 
        ),
        --司机驾驶时长
        driver_time as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                city_id,
                driver_id,
                round(sum(end_time - start_time)/3600, 2) as driver_time            --司机驾驶时长
            from obus_dw_ods.ods_sqoop_data_driver_trip_df 
            where dt = '{pt}' and 
                from_unixtime(end_time, 'yyyy-MM-dd') = '{pt}' and 
                status = 1
            group by city_id, driver_id
        ),
        --收入数据
        income_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                dd.city_id,
                dd.id,
                sum(ddrd.amount_true) as driver_amount,                             ---司机收入
                sum(ddrd.amount_pay_obus) as obus_pay_driver_amount,                ---Obus支付司机收入
                sum(ddrd.amount_pay_ticket) as tickets_pay_driver_amount            --公交卡支付司机收入
            from (select 
                    driver_id,
                    amount_true,
                    amount_pay_obus,
                    amount_pay_ticket
                from obus_dw_ods.ods_sqoop_data_driver_records_day_df 
                where dt='{pt}' and 
                    `day`=unix_timestamp('{pt}','yyyy-MM-dd') 
                ) as ddrd 
            join (select 
                    id,
                    city_id
                from obus_dw_ods.ods_sqoop_data_driver_df 
                where dt='{pt}'
                ) as dd 
            on ddrd.driver_id = dd.id 
            group by dd.city_id, dd.id
        )

        --结果集
        select 
            *,
            row_number() over(partition by city_id order by driver_amount desc) num
        from 
            (select 
                driver_data.dt,
                driver_data.city_id,
                nvl(dc.name, ''),
                driver_data.id,
                driver_data.real_name,
                IF(work_data.work_dur IS NULL, 0, work_data.work_dur),
                IF(driver_cycle_data.cycle_cnt IS NULL, 0, driver_cycle_data.cycle_cnt),
                round(if(driver_cycle_data.cycle_cnt>0, driver_time.driver_time/driver_cycle_data.cycle_cnt, 0), 2),
                IF(income_data.driver_amount IS NULL, 0, income_data.driver_amount) as driver_amount,
                IF(income_data.obus_pay_driver_amount IS NULL, 0, income_data.obus_pay_driver_amount),
                IF(income_data.tickets_pay_driver_amount IS NULL, 0, income_data.tickets_pay_driver_amount)
            from driver_data 
            left join work_data on driver_data.dt=work_data.dt and 
                                    driver_data.city_id=work_data.city_id and 
                                    driver_data.id=work_data.driver_id 
            left join driver_cycle_data on driver_data.dt = driver_cycle_data.dt and 
                                    driver_data.city_id = driver_cycle_data.city_id and 
                                    driver_data.id = driver_cycle_data.driver_id 
            left join driver_time on driver_data.dt = driver_time.dt and 
                                    driver_data.city_id = driver_time.city_id and 
                                    driver_data.id = driver_time.id  
            left join income_data on driver_data.dt = income_data.dt and 
                                    driver_data.city_id = income_data.city_id and 
                                    driver_data.id = income_data.id
            left join (select id, name from obus_dw_ods.ods_sqoop_conf_city_df where dt='{pt}' and validate=1) as dc 
                on driver_data.city_id = dc.id 
            ) as t
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
                    ['dt', 'city_id', 'city', 'driver_id', 'driver_name', 'serv_time', 'cycle_cnt', 'avg_time',
                     'driver_amount', 'obus_pay_driver_amount', 'tickets_pay_driver_amount', 'num'],
                    '''
                        serv_time=values(serv_time),
                        cycle_cnt=values(cycle_cnt),
                        avg_time=values(avg_time),
                        driver_amount=values(driver_amount),
                        obus_pay_driver_amount=values(obus_pay_driver_amount),
                        tickets_pay_driver_amount=values(tickets_pay_driver_amount),
                        num=values(num)
                    '''
                    )

    hive_cursor.close()
    mcursor.close()


def __data_to_mysql(conn, data, column, update=''):
    isql = 'insert into obus_dw_ods.app_obus_report_driver_rank_d ({})'.format(','.join(column))
    esql = '{0} values {1} on duplicate key update {2}'
    sval = ''
    cnt = 0
    try:
        for (dt, city_id, name, id, real_name, work_dur, cycle_cnt, avg_time,
             driver_amount, obus_pay_driver_amount, tickets_pay_driver_amount, num) in data:

            row = [dt, city_id, name, id, real_name, work_dur, cycle_cnt, avg_time,
                        driver_amount, obus_pay_driver_amount, tickets_pay_driver_amount, num]
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


dependence_ods_sqoop_data_driver_df >> sleep_time
dependence_ods_sqoop_data_driver_work_log_df >> sleep_time
dependence_ods_sqoop_data_driver_trip_df >> sleep_time
dependence_ods_sqoop_conf_line_stations_df >> sleep_time
dependence_ods_sqoop_data_driver_records_day_df >> sleep_time

sleep_time >> get_data_from_impala_task
