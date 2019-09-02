"""
10分钟数据监控
"""
import airflow
from plugins.comwx import ComwxApi
import time
import math
from datetime import datetime, timedelta
from utils.connection_helper import get_db_conn
from airflow.operators.python_operator import PythonOperator
import logging

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 7, 16),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_orders_monitor',
    schedule_interval="*/10 * * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)


def data_monitor(**op_kwargs):
    time.sleep(180)
    prev_timepoint = math.floor(int(time.time())/600)*600 - 600
    prev_timestr = time.strftime('%Y-%m-%d %H:%M:00', time.localtime(prev_timepoint))
    bidbconn = get_db_conn('mysql_bi')
    oride_db_conn = get_db_conn('sqoop_db')
    #查询城市列表
    city_sql = '''
        select count(distinct id) from data_city_conf where id < 999000
    '''
    oridedb = oride_db_conn.cursor()
    oridedb.execute(city_sql)
    results = oridedb.fetchone()
    (city_cnt, ) = results
    total_count = (int(city_cnt)+1)*5

    comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')

    #查询当前点数据指标总数
    metrics_sql = '''
        select 
            city_id, city_name, serv_type, order_time, (orders+orders_user+orders_pick+drivers_serv+drivers_orderable+orders_finish+
            avg_pick+avg_take+not_sys_cancel_orders+picked_orders+orders_accept+agg_orders_finish) as total 
        from oride_orders_status_10min where order_time = '{}'
    '''.format(prev_timestr)
    bidb = bidbconn.cursor()

    logging.info(metrics_sql)
    bidb.execute(metrics_sql)
    results = bidb.fetchall()
    metrics_cnt = 0
    for (city_id, city_name, serv_type, order_time, total) in results:
        if city_id >= 999000:
            continue
        metrics_cnt += 1
        if city_id == 0 and serv_type == -1 and total <= 0:
            comwx.postAppMessage('{0}[{1}]10分钟数据{2}数据记录指标全部为0异常，请及时排查，谢谢'.format(city_name,serv_type,order_time), '271')
            return

    if metrics_cnt < total_count:
        comwx.postAppMessage('10分钟数据{0}数据记录缺失异常({1}<{2})，请及时排查，谢谢'.format(
                prev_timestr, metrics_cnt, total_count
            ), '271'
        )
        return

    #检查上2个时间点数据 与 一周前相同时间点对比差异
    weekly_diff = '''
        select 
            t1.city_id, 
            t1.city_name,
            t1.serv_type, 
            t1.order_time, 
            t1.orders as t1orders,
            if(isnull(t2.orders) or t2.orders<=0, 0, t2.orders) as t2orders,
            t1.orders_user as t1ousers,
            if(isnull(t2.orders_user) or t2.orders_user<=0, 0, t2.orders_user) as t2ousers,
            t1.orders_pick as t1opicks,
            if(isnull(t2.orders_pick) or t2.orders_pick<=0, 0, t2.orders_pick) as t2opicks,
            t1.drivers_serv as t1dservs,
            if(isnull(t2.drivers_serv) or t2.drivers_serv<=0, 0, t2.drivers_serv) as t2dservs,
            t1.drivers_orderable as t1doables,
            if(isnull(t2.drivers_orderable) or t2.drivers_orderable<=0, 0, t2.drivers_orderable) as t2doables,
            t1.orders_finish as t1ofs,
            if(isnull(t2.orders_finish) or t2.orders_finish<=0, 0, t2.orders_finish) as t2ofs,
            t1.avg_pick as t1apicks,
            if(isnull(t2.avg_pick) or t2.avg_pick<=0, 0, t2.avg_pick) as t2apicks,
            t1.avg_take as t1atakes,
            if(isnull(t2.avg_take) or t2.avg_take<=0, 0, t2.avg_take) as t2atakes,
            t1.not_sys_cancel_orders as t1norders,
            if(isnull(t2.not_sys_cancel_orders) or t2.not_sys_cancel_orders<=0, 0, t2.not_sys_cancel_orders) as t2norders,
            t1.picked_orders as t1pos,
            if(isnull(t2.picked_orders) or t2.picked_orders<=0, 0, t2.picked_orders) as t2pos,
            t1.agg_orders_finish as t1aofs,
            if(isnull(t2.agg_orders_finish) or t2.agg_orders_finish<=0, 0, t2.agg_orders_finish) as t2aofs
        from
            (select * from oride_orders_status_10min where order_time>=from_unixtime({dsb2})) t1 
        left join 
            (select * from oride_orders_status_10min where order_time>=from_unixtime({dsb7}) and order_time<=from_unixtime({dsb7a3})) t2 
        on 
            t1.city_id = t2.city_id and 
            t1.serv_type = t2.serv_type and 
            t1.order_time = date_format(from_unixtime(unix_timestamp(t2.order_time)+86400*7), '%Y-%m-%d %H:%i:00')
    '''.format(
        dsb2=prev_timepoint-1200,
        dsb7=prev_timepoint-1200-86400*7,
        dsb7a3=prev_timepoint-86400*7
    )
    logging.info(weekly_diff)
    bidb.execute(weekly_diff)
    results = bidb.fetchall()
    for (city_id, city_name, serv_type, order_time, t1orders, t2orders, t1ousers, t2ousers, t1opicks, t2opicks,
         t1dservs, t2dservs, t1doables, t2doables, t1ofs, t2ofs, t1apicks, t2apicks, t1atakes, t2atakes,
         t1norders, t2norders, t1pos, t2pos, t1aofs, t2aofs) in results:
        if serv_type == -1 and ((t2orders >= 100 and t2orders > t1orders and (t2orders - t1orders)/t2orders > 0.8) or \
                (t2orders > 0 and t2orders < 100 and (t2orders - t1orders) > 40)):
            comwx.postAppMessage('{0}[{1}]10分钟数据{2}下单数记录与上周同期对比异常，请及时排查，谢谢'.format(city_name, serv_type, order_time),
                                 '271')
            return

        if serv_type == -1 and ((t2dservs >= 200 and t2dservs > t1dservs and (t2dservs - t1dservs)/t2dservs > 0.8) or \
                (t2dservs > 0 and t2dservs < 100 and (t2dservs - t1dservs) > 80)):
            comwx.postAppMessage('{0}[{1}]10分钟数据{2}在线司机数记录与上周同期对比异常，请及时排查，谢谢'.format(city_name, serv_type, order_time),
                                 '271')
            return

        if serv_type == -1 and ((t2doables >= 200 and t2doables > t1doables and (t2doables - t1doables)/t2doables > 0.8) or \
                (t2doables > 0 and t2doables < 100 and (t2doables - t1doables) > 80)):
            comwx.postAppMessage('{0}[{1}]10分钟数据{2}可接单司机数记录与上周同期对比异常，请及时排查，谢谢'.format(city_name, serv_type, order_time),
                                 '271')
            return


data_monitor_task = PythonOperator(
    task_id='data_monitor_task',
    python_callable=data_monitor,
    provide_context=True,
    dag=dag
)

data_monitor_task
