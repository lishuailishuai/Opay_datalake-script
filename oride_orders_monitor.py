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
    'owner': 'root',
    'start_date': datetime(2019, 7, 16),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
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
    time.sleep(60)
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
            city_id, serv_type, order_time, (orders+orders_user+orders_pick+drivers_serv+drivers_orderable+orders_finish+
            avg_pick+avg_take+not_sys_cancel_orders+picked_orders+orders_accept+agg_orders_finish) as total 
        from oride_orders_status_10min where order_time = '{}'
    '''.format(prev_timestr)
    bidb = bidbconn.cursor()

    logging.info(metrics_sql)
    bidb.execute(metrics_sql)
    results = bidb.fetchall()
    metrics_cnt = 0
    for (city_id, serv_type, order_time, total) in results:
        if city_id >= 999000:
            continue
        metrics_cnt += 1
        if city_id != 1002 and serv_type != 0 and serv_type != 1 and serv_type != 99 and total <= 0:
            pass
            logging.info('10分钟数据{}数据记录指标全部为0异常，请及时排查，谢谢'.format(order_time))
            comwx.postAppMessage('10分钟数据{}数据记录指标全部为0异常，请及时排查，谢谢'.format(order_time), '271', '')
            return

    if metrics_cnt < total_count:
        pass
        logging.info('10分钟数据{0}数据记录缺失异常({1}<{2})，请及时排查，谢谢'.format(prev_timestr, metrics_cnt, total_count))
        comwx.postAppMessage('10分钟数据{0}数据记录缺失异常({1}<{2})，请及时排查，谢谢'.format(
                prev_timestr, metrics_cnt, total_count
            ), '271', ''
        )
        return

    #检查上2个时间点数据 与 一周前相同时间点对比差异
    weekly_diff = '''
        select 
            t1.city_id, 
            t1.serv_type, 
            t1.order_time, 
            if(isnull(t2.orders) or t2.orders<=0, 0, if(t1.orders>=t2.orders, 0, t2.orders-t1.orders)/t2.orders) as orders,
            if(isnull(t2.orders_user) or t2.orders_user<=0, 0, if(t1.orders_user>=t2.orders_user, 0, t2.orders_user-t1.orders_user)/t2.orders_user) as orders_user,
            if(isnull(t2.orders_pick) or t2.orders_pick<=0, 0, if(t1.orders_pick>=t2.orders_pick, 0, t2.orders_pick-t1.orders_pick)/t2.orders_pick) as orders_pick,
            if(isnull(t2.drivers_serv) or t2.drivers_serv<=0, 0, if(t1.drivers_serv>=t2.drivers_serv, 0, t2.drivers_serv-t1.drivers_serv)/t2.drivers_serv) as drivers_serv,
            if(isnull(t2.drivers_orderable) or t2.drivers_orderable<=0, 0, if(t1.drivers_orderable>=t2.drivers_orderable, 0, t2.drivers_orderable-t1.drivers_orderable)/t2.drivers_orderable) as drivers_orderable,
            if(isnull(t2.orders_finish) or t2.orders_finish<=0, 0, if(t1.orders_finish>=t2.orders_finish, 0, t2.orders_finish-t1.orders_finish)/t2.orders_finish) as orders_finish,
            if(isnull(t2.avg_pick) or t2.avg_pick<=0, 0, if(t1.avg_pick>=t2.avg_pick, t1.avg_pick-t2.avg_pick, 0)/t2.avg_pick) as avg_pick,
            if(isnull(t2.avg_take) or t2.avg_take<=0, 0, if(t1.avg_take>=t2.avg_take, t1.avg_take-t2.avg_take, 0)/t2.avg_take) as avg_take,
            if(isnull(t2.not_sys_cancel_orders) or t2.not_sys_cancel_orders<=0, 0, if(t1.not_sys_cancel_orders>=t2.not_sys_cancel_orders, 0, t2.not_sys_cancel_orders-t1.not_sys_cancel_orders)/t2.not_sys_cancel_orders) as not_sys_cancel_orders,
            if(isnull(t2.picked_orders) or t2.picked_orders<=0, 0, if(t1.picked_orders>=t2.picked_orders, 0, t2.picked_orders-t1.picked_orders)/t2.picked_orders) as picked_orders,
            if(isnull(t2.orders_accept) or t2.orders_accept<=0, 0, if(t1.orders_accept>=t2.orders_accept, 0, t2.orders_accept-t1.orders_accept)/t2.orders_accept) as orders_accept,
            if(isnull(t2.agg_orders_finish) or t2.agg_orders_finish<=0, 0, if(t1.agg_orders_finish>=t2.agg_orders_finish, 0, t2.agg_orders_finish-t1.agg_orders_finish)/t2.agg_orders_finish) as agg_orders_finish
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
    for (city,type,otime,orders,ousers,opicks,dservs,dorders,ofinishs,apicks,atakes,norders,porders,oaccepts,aofinishs) in results:
        if orders >= 0.5 or ousers >= 0.5 or opicks >= 0.5 or dservs >= 0.5 or dorders >= 0.5 or \
                norders >= 0.5 or aofinishs >= 0.5 or apicks >= 0.5 or atakes >= 0.5:
            pass
            logging.info('10分钟数据{0}数据记录与上周对比异常，请及时排查，谢谢'.format(otime))
            comwx.postAppMessage('10分钟数据{0}数据记录与上周对比异常，请及时排查，谢谢'.format(otime), '271', '')
            return


data_monitor_task = PythonOperator(
    task_id='data_monitor_task',
    python_callable=data_monitor,
    provide_context=True,
    dag=dag
)

data_monitor_task
