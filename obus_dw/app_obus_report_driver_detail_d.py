"""
obus 线路明细
"""
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.operators.impala_plugin import ImpalaOperator
from datetime import datetime, timedelta
from utils.connection_helper import get_hive_cursor, get_db_conn, get_db_conf
from utils.validate_metrics_utils import *
#from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
from airflow.operators.bash_operator import BashOperator
import time
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 8, 18),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'app_obus_report_driver_detail_d',
    schedule_interval="00 01 * * *",
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

dependence_ods_sqoop_conf_cycle_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_conf_cycle_df',
    prefix='obus_dw/ods_sqoop_conf_cycle_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_data_driver_work_log_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_data_driver_work_log_df',
    prefix='obus_dw/ods_sqoop_data_driver_work_log_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
    bucket_name='opay-bi',
    dag=dag
)

dependence_ods_sqoop_data_order_df = S3PrefixSensor(
    task_id='dependence_ods_sqoop_data_order_df',
    prefix='obus_dw/ods_sqoop_data_order_df/country_code=nal/dt={pt}'.format(pt='{{ ds }}'),
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
        --司机数据
        driver_data as 
        (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                dd.id as driver_id,
                dd.real_name as driver_name,                                    --司机名字
                dd.phone_number as driver_phone,                                --司机电话
                dd.plate_number as driver_bus_number,                           --车牌号
                dd.cycle_id,                                                    ---环线代号
                cc.name as cycle_name,                                           --所属线路
                0 as number_of_seats                                            --座位数
            from (select 
                    id,
                    real_name,
                    phone_number,
                    plate_number,
                    cycle_id
                from obus_dw.ods_sqoop_data_driver_df 
                where dt='{pt}'
                ) as dd 
            left join (select 
                    id,
                    `name`
                from obus_dw.ods_sqoop_conf_cycle_df 
                where dt='{pt}'
                ) as cc 
            on dd.cycle_id = cc.id
        ),
        --工作数据
        work_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                dr.id as driver_id,
                sum(if(dw.serv_mode=1 and dw.serv_mode1=0, round(abs(dw.create_time2-dw.create_time)/3600,2), 0)) as work_dur             --司机今日在线时长(小时)
            from 
                (select 
                    driver_id,
                    serv_mode,
                    create_time,
                    lead(serv_mode,1,0) over(partition by driver_id order by create_time) serv_mode1,
                    lead(create_time,1,unix_timestamp('{pt} 23:59:59','yyyy-MM-dd HH:mm:ss')) over(partition by driver_id order by create_time) create_time2
                from obus_dw.ods_sqoop_data_driver_work_log_df 
                where dt='{pt}' and 
                    from_unixtime(create_time, 'yyyy-MM-dd')='{pt}'
                ) as dw 
            join (select 
                    id
                from obus_dw.ods_sqoop_data_driver_df 
                where dt='{pt}'
                ) as dr 
            on dw.driver_id = dr.id 
            group by dr.id
        ),
        --订单数据
        order_data as (
            select 
                from_unixtime(unix_timestamp('{pt}','yyyy-MM-dd'), 'yyyyMMdd') as dt,
                driver_id,
                count(1) as orders,                                         ---本日已经完成的订单数
                sum(price) as mtd_gmv_today                                 ---本日累计交易额
            from obus_dw.ods_sqoop_data_order_df 
            where dt='{pt}' and 
                from_unixtime(create_time, 'yyyy-MM-dd') = '{pt}' and 
                status in (1,2)
            group by driver_id
        )
        
        --结果集
        select 
            *,
            row_number() over(partition by null order by driver_id) num
        from 
            (select 
                driver_data.dt,
                driver_data.driver_id,
                driver_data.driver_name,
                driver_data.driver_phone,
                driver_data.driver_bus_number,
                driver_data.cycle_id,
                nvl(driver_data.cycle_name, ''),
                driver_data.number_of_seats,
                nvl(work_data.work_dur, 0),
                nvl(order_data.orders, 0),
                nvl(order_data.mtd_gmv_today, 0)
            from driver_data
            left join work_data on driver_data.dt=work_data.dt and 
                                    driver_data.driver_id=work_data.driver_id 
            left join order_data on driver_data.dt = order_data.dt and 
                                    driver_data.driver_id = order_data.driver_id 
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
                    ['dt', 'num', 'driver_id', 'driver_name', 'driver_phone', 'driver_bus_number', 'cycle_id', 'cycle_name',
                     'number_of_seats', 'mtd_serv_time_today', 'finished_orders_today', 'mtd_gmv_today'],
                    '''
                        num=values(num),
                        driver_name=values(driver_name),
                        driver_phone=values(driver_phone),
                        driver_bus_number=values(driver_bus_number),
                        cycle_id=values(cycle_id),
                        cycle_name=values(cycle_name),
                        number_of_seats=values(number_of_seats),
                        mtd_serv_time_today=values(mtd_serv_time_today),
                        finished_orders_today=values(finished_orders_today),
                        mtd_gmv_today=values(mtd_gmv_today)
                    '''
                    )


def __data_to_mysql(conn, data, column, update=''):
    isql = 'insert into obus_dw.app_obus_report_driver_detail_d ({})'.format(','.join(column))
    esql = '{0} values {1} on duplicate key update {2}'
    sval = ''
    cnt = 0
    try:
        for (dt, driver_id, driver_name, driver_phone, driver_bus_number, cycle_id, cycle_name,
                number_of_seats, work_dur, orders, mtd_gmv_today, num) in data:

            row = [dt, num, driver_id, driver_name, driver_phone, driver_bus_number, cycle_id, cycle_name,
                        number_of_seats, work_dur, orders, mtd_gmv_today]
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
dependence_ods_sqoop_conf_cycle_df >> sleep_time
dependence_ods_sqoop_data_driver_work_log_df >> sleep_time
dependence_ods_sqoop_data_order_df >> sleep_time

sleep_time >> get_data_from_impala_task
