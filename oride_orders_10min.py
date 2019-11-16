'''
add by duo.wu 下单量、下单人数、接单量、在线司机数，从业务从库读取数据
'''

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.redis_hook import RedisHook
from utils.connection_helper import get_hive_cursor, get_db_conn, get_pika_connection
from datetime import timedelta, time, datetime
from utils.connection_helper import get_db_conf
import os
import time
import pandas as pd
import logging
from collections import OrderedDict
import math

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 5, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_orders_10min',
    schedule_interval="*/10 * * * *",
    default_args=args
)

active_a_driver = "algo:active_driver:a:%s"    # "active_driver:a:%s"
active_no_driver = "algo:active_driver:no:%s"   # "active_driver:no:%s"

insert_driver_num = """
replace into bi.driver_online values (%s,%s,%s,%s,%s)
"""

query_driver_city_serv = '''
select id,serv_type,city_id from data_driver_extend where id > {id} order by id limit 1000
'''

create_oride_orders_status = MySqlOperator(
    task_id='create_oride_orders_status',
    sql="""
        CREATE TABLE IF NOT EXISTS oride_orders_status_10min (
            `city_id` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '城市ID',
            `city_name` varchar(64) NOT NULL DEFAULT '' COMMENT '城市名称',
            `serv_type` int(11) NOT NULL DEFAULT '-1' COMMENT '服务类型1专车2快车',
            `order_time` timestamp not null default '1970-01-02 00:00:00' comment 'time 10min',
            `daily` timestamp not null default '1970-01-02 00:00:00' comment 'time day',
            `orders` int unsigned not null default 0 comment '下单数',
            `orders_user` int unsigned not null default 0 comment '下单用户数',
            `orders_pick` int unsigned not null default 0 comment '订单业务接单数',
            `orders_pick_dserv` int unsigned not null default 0 comment '按司机业务接单数',
            `drivers_serv` int unsigned not null default 0 comment '在线司机数',
            `drivers_orderable` int(10) unsigned NOT NULL DEFAULT '0' COMMENT '可接单司机数',
            `orders_finish` int unsigned not null default 0 comment '订单业务完单数',
            `orders_finish_dserv` int unsigned not null default 0 comment '司机业务完单数',
            `orders_finish_create` int unsigned not null default 0 comment '订单业务基于创建时间的完单数',
            `avg_pick` int unsigned not null default 0 comment '订单业务平均应答时长(秒)take_time-create_time',
            `avg_pick_dserv` int unsigned not null default 0 comment '司机业务平均应答时长(秒)take_time-create_time',
            `avg_take` decimal(10,1) unsigned not null default '0.0' comment '订单业务平均接驾时长(分)pick_time-take_time',
            `avg_take_dserv` decimal(10,1) unsigned not null default '0.0' comment '司机业务平均接驾时长(分)pick_time-take_time',
            `not_sys_cancel_orders` int unsigned not null default 0 comment '订单业务应答后取消status = 6 and driver_id > 0 and cancel_role <> 3 and cancel_role <> 4',
            `not_sys_cancel_orders_dserv` int unsigned not null default 0 comment '司机业务应答后取消status = 6 and driver_id > 0 and cancel_role <> 3 and cancel_role <> 4',
            `picked_orders` int unsigned not null default 0 comment '订单业务成功接驾',
            `picked_orders_dserv` int unsigned not null default 0 comment '司机业务成接驾',
            `orders_accept` int unsigned not null default 0 comment '订单业务接单数',
            `orders_accept_dserv` int unsigned not null default 0 comment '司机业务接单数',
            `agg_orders_finish` int unsigned not null default 0 comment '订单业务累计完单数',
            `agg_orders_finish_dserv` int unsigned not null default 0 comment '司机业务累计完单数',
            primary key (`city_id`,`serv_type`,`order_time`)
        ) engine=innodb DEFAULT CHARSET=utf8;
    """,
    database='bi',
    mysql_conn_id='mysql_bi',
    dag=dag
)

bidb_conn = get_db_conn('mysql_bi')
bidb = bidb_conn.cursor()
oridedb_conn = get_db_conn('sqoop_db')

driver_type = '-1,0,1,2,99'

"""
预插入统计时间节点
@:param op_kwargs 
"""
def preInsertRowPoint(**op_kwargs):
    test_mode = op_kwargs.get('test_mode', False)
    if test_mode:
        ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time())))
        prev_day_start = int(time.mktime(datetime.strptime(ds, '%Y-%m-%d').timetuple()))
        prev_day_end = prev_day_start + 86400
    else:
        prev_day_start = math.floor(int(time.time()) / 600) * 600 - 600
        prev_day_end = prev_day_start + 600

    msql = '''
        select 
            id, 
            name, 
            \'{0}\' as driver_type 
        from data_city_conf
    '''.format(driver_type)
    citys = pd.read_sql_query(msql, oridedb_conn)
    citys.loc['n'] = ['0', 'all', driver_type]
    citys = citys.join(citys['driver_type'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('driver_type2'))
    citys = citys.drop('driver_type', axis=1)

    daily = time.strftime('%Y-%m-%d 00:00:00', time.localtime(prev_day_start))
    results = pd.DataFrame(columns=['id', 'name', 'driver_type2', 'order_time', 'daily'])
    while prev_day_start < prev_day_end:
        temp = citys
        temp['order_time'] = temp['id'].map(lambda x: time.strftime('%Y-%m-%d %H:%M:00', time.localtime(prev_day_start)))
        temp['daily'] = daily
        prev_day_start += 600
        results = results.append(temp, ignore_index=True)

    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['city_id', 'city_name', 'serv_type', 'order_time', 'daily'],
        'city_name=values(city_name)'
    )


"""
计算数据指标
@:param op_kwargs
"""
def metricsCount(**op_kwargs):
    test_mode = op_kwargs.get('test_mode', False)
    if test_mode:
        ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time())))
        prev_day_start = int(time.mktime(datetime.strptime(ds, '%Y-%m-%d').timetuple()))
        prev_day_end = prev_day_start + 86400
    else:
        prev_day_start = math.floor(int(time.time()) / 600) * 600 - 600
        prev_day_end = prev_day_start + 600

    #数据指标计算
    __getOrideOrders(prev_day_start, prev_day_end, test_mode)
    __getOrideTakes(prev_day_start, prev_day_end)
    __getDriversOnline(prev_day_start-600, prev_day_end)
    __getOrderFinished(prev_day_start, prev_day_end)
    __oggOrderFinished(prev_day_start, prev_day_end, test_mode)
    __getOrderPicks(prev_day_start, prev_day_end)
    __getOrderCanel(prev_day_start, prev_day_end)

    __finally()


"""
查询符合条件的订单记录
@:param st 
@:param ed
"""
def __getOrideOrders(st, ed, recover=False):
    if not recover:
        st -= 7200
    msql = '''
        select 
            id,
            user_id,
            city_id,
            serv_type,
            if(status=4 or status=5, 1, 0) as orders_finish,
            date_format(from_unixtime(floor(create_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time, 
            date_format(from_unixtime(create_time), '%Y-%m-%d 00:00:00') as order_day 
        from oride_data.data_order 
        where create_time >= {st} and create_time < {ed} 
    '''.format(st=st, ed=ed)
    logging.info(msql)
    orders = pd.read_sql_query(msql, oridedb_conn)
    #全国
    orders_all = orders.groupby(['order_time']).agg(
        OrderedDict([
            ('id', 'count'),
            ('orders_finish', 'sum'),
            ('user_id', 'nunique'),
            ('order_time', 'max'),
            ('order_day', 'max')
        ])
    )
    orders_all['city_id'] = orders_all['id'].map(lambda x: 0)
    orders_all['serv_type'] = orders_all['id'].map(lambda x: -1)
    #城市
    orders_city = orders.groupby(['order_time', 'city_id']).agg(
        OrderedDict([
            ('id', 'count'),
            ('orders_finish', 'sum'),
            ('user_id', 'nunique'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max')
        ])
    )
    orders_city['serv_type'] = orders_city['id'].map(lambda x: -1)
    #类型
    orders_type = orders.groupby(['order_time', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('orders_finish', 'sum'),
            ('user_id', 'nunique'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    orders_type['city_id'] = orders_type['id'].map(lambda x: 0)
    #城市、类型
    orders_ctype = orders.groupby(['order_time', 'city_id', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('orders_finish', 'sum'),
            ('user_id', 'nunique'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    results = orders_all.append(orders_city, ignore_index=True).append(orders_ctype, ignore_index=True).append(orders_type, ignore_index=True)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['orders', 'orders_finish_create', 'orders_user', 'order_time', 'daily', 'city_id', 'serv_type'],
        'orders=values(orders),orders_user=values(orders_user),orders_finish_create=values(orders_finish_create)'
    )


"""
接单数、平均接单时长
@:param st
@:param ed
"""
def __getOrideTakes(st, ed):
    msql = '''
        select 
            id,
            city_id, 
            serv_type as serv_type,
            driver_serv_type as dserv_type,
            if(take_time>create_time, take_time-create_time, 0) as take_range,
            date_format(from_unixtime(floor(take_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time, 
            date_format(from_unixtime(take_time), '%Y-%m-%d 00:00:00') as order_day 
        from oride_data.data_order 
        where take_time >= {st} and take_time < {ed} and driver_id > 0 
    '''.format(st=st, ed=ed)
    logging.info(msql)
    takes = pd.read_sql_query(msql, oridedb_conn)
    #全国
    takes_all = takes.groupby(['order_time']).agg(
        OrderedDict([
            ('id', 'count'),
            ('take_range', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max')
        ])
    )
    takes_all['city_id'] = takes_all['id'].map(lambda x: 0)
    takes_all['serv_type'] = takes_all['id'].map(lambda x: -1)
    #城市
    takes_city = takes.groupby(['order_time', 'city_id']).agg(
        OrderedDict([
            ('id', 'count'),
            ('take_range', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max')
        ])
    )
    takes_city['serv_type'] = takes_city['id'].map(lambda x: -1)
    #订单类型
    takes_type = takes.groupby(['order_time', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('take_range', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    takes_type['city_id'] = takes_type['id'].map(lambda x: 0)
    #订单城市、类型
    takes_ctype = takes.groupby(['order_time', 'city_id', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('take_range', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    results = takes_all.append(takes_city, ignore_index=True).append(takes_type, ignore_index=True).append(takes_ctype, ignore_index=True)
    if len(results) > 0:
        results['take_range'] = results.apply(lambda x: math.floor(x['take_range'] / x['id']) if x['id'] != 0 else 0, axis=1)
    results['orders_accept'] = results['id']
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['orders_pick', 'avg_pick', 'order_time', 'daily', 'city_id', 'serv_type', 'orders_accept'],
        'orders_pick=values(orders_pick),orders_accept=values(orders_accept),avg_pick=values(avg_pick)'
    )
    #-----------------------
    #司机类型
    takes_dtype = takes.groupby(['order_time', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('take_range', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    takes_dtype['city_id'] = takes_dtype['id'].map(lambda x: 0)
    #司机城市、类型 orders_pick_dserv
    takes_dctype = takes.groupby(['order_time', 'city_id', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('take_range', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    results = takes_dtype.append(takes_dctype, ignore_index=True)
    if len(results) > 0:
        results['take_range'] = results.apply(lambda x: math.floor(x['take_range'] / x['id']) if x['id'] != 0 else 0, axis=1)
    results['orders_accept_dserv'] = results['id']
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['orders_pick_dserv', 'avg_pick_dserv', 'order_time', 'daily', 'city_id', 'serv_type', 'orders_accept_dserv'],
        'orders_pick_dserv=values(orders_pick_dserv),orders_accept_dserv=values(orders_accept_dserv),avg_pick_dserv=values(avg_pick_dserv)'
    )



"""
在线司机数、可接单司机数
@:param st
@:param ed
"""
def __getDriversOnline(st, ed):
    msql = '''
        select 
            City as city_id,
            type as serv_type,
            online_time,
            date_format(online_time, '%Y-%m-%d 00:00:00') as daily,
            drivers_online,
            driver_orderable 
        from driver_online 
        where online_time >= from_unixtime({st}) and online_time < from_unixtime({ed}) 
    '''.format(st=st, ed=ed)
    logging.info(msql)
    drivers = pd.read_sql_query(msql, bidb_conn)
    #全国
    drivers_all = drivers.groupby(['online_time']).agg(
        OrderedDict([
            ('city_id', 'max'),
            ('serv_type', 'max'),
            ('online_time', 'max'),
            ('daily', 'max'),
            ('drivers_online', 'sum'),
            ('driver_orderable', 'sum')
        ])
    )
    drivers_all['city_id'] = drivers_all['online_time'].map(lambda x: 0)
    drivers_all['serv_type'] = drivers_all['online_time'].map(lambda x: -1)
    #城市
    drivers_city = drivers.groupby(['online_time', 'city_id']).agg(
        OrderedDict([
            ('city_id', 'max'),
            ('serv_type', 'max'),
            ('online_time', 'max'),
            ('daily', 'max'),
            ('drivers_online', 'sum'),
            ('driver_orderable', 'sum')
        ])
    )
    drivers_city['serv_type'] = drivers_city['online_time'].map(lambda x: -1)
    #类型
    drivers_type = drivers.groupby(['online_time', 'serv_type']).agg(
        OrderedDict([
            ('city_id', 'max'),
            ('serv_type', 'max'),
            ('online_time', 'max'),
            ('daily', 'max'),
            ('drivers_online', 'sum'),
            ('driver_orderable', 'sum')
        ])
    )
    drivers_type['city_id'] = drivers_type['online_time'].map(lambda x: 0)
    #城市、类型
    results = drivers.append(drivers_all, ignore_index=True).append(drivers_city, ignore_index=True).append(drivers_type, ignore_index=True)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['city_id', 'serv_type', 'order_time', 'daily', 'drivers_serv', 'drivers_orderable'],
        'drivers_serv=values(drivers_serv),drivers_orderable=values(drivers_orderable)'
    )


"""
完成订单数
@:param st
@:param ed
"""
def __getOrderFinished(st, ed):
    msql = '''
        select 
            id,
            city_id, 
            serv_type as serv_type,
            driver_serv_type as dserv_type,
            date_format(from_unixtime(floor(arrive_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time, 
            date_format(from_unixtime(arrive_time), '%Y-%m-%d 00:00:00') as order_day 
        from oride_data.data_order 
        where arrive_time >= {st} and arrive_time < {ed} and status in (4,5)
    '''.format(st=st, ed=ed)
    logging.info(msql)
    finished = pd.read_sql_query(msql, oridedb_conn)
    #全国
    finished_all = finished.groupby(['order_time']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max')
        ])
    )
    finished_all['city_id'] = finished_all['id'].map(lambda x: 0)
    finished_all['serv_type'] = finished_all['id'].map(lambda x: -1)
    #城市
    finished_city = finished.groupby(['order_time', 'city_id']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max')
        ])
    )
    finished_city['serv_type'] = finished_city['id'].map(lambda x: -1)
    #订单类型
    finished_type = finished.groupby(['order_time', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    finished_type['city_id'] = finished_type['id'].map(lambda x: 0)
    #订单城市、类型
    finished_ctype = finished.groupby(['order_time', 'city_id', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )

    results = finished_all.append(finished_city, ignore_index=True).append(finished_type, ignore_index=True).append(finished_ctype, ignore_index=True)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['orders_finish', 'order_time', 'daily', 'city_id', 'serv_type'],
        'orders_finish=values(orders_finish)'
    )

    #-------------------
    #司机类型
    finished_dtype = finished.groupby(['order_time', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    finished_dtype['city_id'] = finished_dtype['id'].map(lambda x: 0)
    #司机城市、类型
    finished_dctype = finished.groupby(['order_time', 'city_id', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    results = finished_dtype.append(finished_dctype, ignore_index=True)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['orders_finish_dserv', 'order_time', 'daily', 'city_id', 'serv_type'],
        'orders_finish_dserv=values(orders_finish_dserv)'
    )


"""
累计完单数
@:param st
@:param ed
@:param recover
"""
def __oggOrderFinished(st, ed, recover=False):
    oggfinished_all = pd.DataFrame(columns=['order_time', 'order_day', 'ogg_orders_finish', 'agg_orders_finish_dserv', 'city_id', 'serv_type'])

    if recover:
        et = st
    else:
        et = ed - 600
        st = int(time.mktime(datetime.strptime(time.strftime('%Y-%m-%d', time.localtime(st)), '%Y-%m-%d').timetuple()))
    while et < ed:
        msql = '''
            select 
                date_format(from_unixtime(floor({et}/600)*600), '%Y-%m-%d %H:%i:00') as order_time, 
                date_format(from_unixtime({et}), '%Y-%m-%d 00:00:00') as order_day, 
                sum(orders_finish) as ogg_orders_finish,
                sum(orders_finish_dserv) as agg_orders_finish_dserv, 
                city_id,
                serv_type
            from oride_orders_status_10min 
            where order_time >= from_unixtime({st}) and 
                order_time < from_unixtime({et}+600) 
            group by city_id, serv_type 
        '''.format(st=st, et=et)
        logging.info(msql)
        et += 600
        oggfinished = pd.read_sql_query(msql, bidb_conn)
        oggfinished_all = oggfinished_all.append(oggfinished, ignore_index=True)


    __data_to_mysql(
        bidb,
        oggfinished_all.values.tolist(),
        ['order_time', 'daily', 'agg_orders_finish', 'agg_orders_finish_dserv', 'city_id', 'serv_type'],
        'agg_orders_finish=values(agg_orders_finish),agg_orders_finish_dserv=values(agg_orders_finish_dserv)'
    )


"""
接到乘客订单数，平均接驾时长
@:param st
@:param ed
"""
def __getOrderPicks(st, ed):
    msql = '''
        select 
            id,
            if(pickup_time>take_time, pickup_time-take_time, 0) as pickrange,
            city_id,
            serv_type as serv_type,
            driver_serv_type as dserv_type,
            date_format(from_unixtime(floor(pickup_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time, 
            date_format(from_unixtime(pickup_time), '%Y-%m-%d 00:00:00') as order_day 
        from oride_data.data_order 
        where pickup_time >= {st} and pickup_time < {ed}
    '''.format(st=st, ed=ed)
    logging.info(msql)
    picks = pd.read_sql_query(msql, oridedb_conn)
    #全国
    picks_all = picks.groupby(['order_time']).agg(
        OrderedDict([
            ('id', 'count'),
            ('pickrange', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max')
        ])
    )
    picks_all['city_id'] = picks_all['id'].map(lambda x: 0)
    picks_all['serv_type'] = picks_all['id'].map(lambda x: -1)
    #城市
    picks_city = picks.groupby(['order_time', 'city_id']).agg(
        OrderedDict([
            ('id', 'count'),
            ('pickrange', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max')
        ])
    )
    picks_city['serv_type'] = picks_city['id'].map(lambda x: -1)
    #订单类型
    picks_type = picks.groupby(['order_time', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('pickrange', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    picks_type['city_id'] = picks_type['id'].map(lambda x: 0)
    #订单城市、类型
    picks_ctype = picks.groupby(['order_time', 'city_id', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('pickrange', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    results = picks_all.append(picks_city, ignore_index=True).append(picks_type, ignore_index=True).append(picks_ctype, ignore_index=True)
    if len(results) > 0:
        results['pickrange'] = results.apply(lambda x: round(x['pickrange']/x['id']/60, 1) if x['id'] != 0 else 0, axis=1)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['picked_orders', 'avg_take', 'order_time', 'daily', 'city_id', 'serv_type'],
        'picked_orders=values(picked_orders),avg_take=values(avg_take)'
    )

    #-----------------
    #司机类型
    picks_dtype = picks.groupby(['order_time', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('pickrange', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    picks_dtype['city_id'] = picks_dtype['id'].map(lambda x: 0)
    #司机城市、类型
    picks_dctype = picks.groupby(['order_time', 'city_id', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('pickrange', 'sum'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    results = picks_dtype.append(picks_dctype, ignore_index=True)
    if len(results) > 0:
        results['pickrange'] = results.apply(lambda x: round(x['pickrange'] / x['id'] / 60, 1) if x['id'] != 0 else 0, axis=1)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['picked_orders_dserv', 'avg_take_dserv', 'order_time', 'daily', 'city_id', 'serv_type'],
        'picked_orders_dserv=values(picked_orders_dserv),avg_take_dserv=values(avg_take_dserv)'
    )


"""
接单取消数
@:param st
@:param ed
"""
def __getOrderCanel(st, ed):
    msql = '''
        select 
            id,
            city_id,
            serv_type as serv_type,
            driver_serv_type as dserv_type,
            date_format(from_unixtime(floor(cancel_time/600)*600), '%Y-%m-%d %H:%i:00') as order_time, 
            date_format(from_unixtime(cancel_time), '%Y-%m-%d 00:00:00') as order_day 
        from oride_data.data_order 
        where cancel_time >= {st} and 
            cancel_time < {ed} and 
            status = 6 and 
            driver_id > 0 and 
            cancel_role <> 3 and 
            cancel_role <> 4 
    '''.format(st=st, ed=ed)
    logging.info(msql)
    canels = pd.read_sql_query(msql, oridedb_conn)
    #全国
    canels_all = canels.groupby(['order_time']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max')
        ])
    )
    canels_all['city_id'] = canels_all['id'].map(lambda x: 0)
    canels_all['serv_type'] = canels_all['id'].map(lambda x: -1)
    #城市
    canels_city = canels.groupby(['order_time', 'city_id']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max')
        ])
    )
    canels_city['serv_type'] = canels_city['id'].map(lambda x: -1)
    #订单类型
    canels_type = canels.groupby(['order_time', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    canels_type['city_id'] = canels_type['id'].map(lambda x: 0)
    #订单城市、类型
    canels_ctype = canels.groupby(['order_time', 'city_id', 'serv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('serv_type', 'max')
        ])
    )
    results = canels_all.append(canels_city, ignore_index=True).append(canels_type, ignore_index=True).append(canels_ctype, ignore_index=True)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['not_sys_cancel_orders', 'order_time', 'daily', 'city_id', 'serv_type'],
        'not_sys_cancel_orders=values(not_sys_cancel_orders)'
    )

    #----------------
    #司机类型
    canels_dtype = canels.groupby(['order_time', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    canels_dtype['city_id'] = canels_dtype['id'].map(lambda x: 0)
    #司机城市、类型
    canels_dctype = canels.groupby(['order_time', 'city_id', 'dserv_type']).agg(
        OrderedDict([
            ('id', 'count'),
            ('order_time', 'max'),
            ('order_day', 'max'),
            ('city_id', 'max'),
            ('dserv_type', 'max')
        ])
    )
    results = canels_dtype.append(canels_dctype, ignore_index=True)
    #logging.info(results)
    __data_to_mysql(
        bidb,
        results.values.tolist(),
        ['not_sys_cancel_orders_dserv', 'order_time', 'daily', 'city_id', 'serv_type'],
        'not_sys_cancel_orders_dserv=values(not_sys_cancel_orders_dserv)'
    )


"""
关闭数据库连接
"""
def __finally():
    oridedb_conn.close()
    bidb_conn.close()


"""
存储数据到mysql
@:param conn mysql连接 
@:param data 数据list
@:param column 插入的列
@:param ignore 是否忽略
@:param update 更新列
"""
def __data_to_mysql(conn, data, column, update=''):
    isql = 'insert into oride_orders_status_10min ({})'.format(','.join(column))
    esql = '{0} values {1} on duplicate key update {2}'
    sval = ''
    cnt = 0
    try:
        for row in data:
            if sval == '':
                sval = '(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            else:
                sval += ',(\'{}\')'.format('\',\''.join([str(x) for x in row]))
            cnt += 1
            if cnt >= 1000:
                conn.execute(esql.format(isql, sval, update))
                cnt = 0
                sval = ''

        if cnt > 0 and sval != '':
            conn.execute(esql.format(isql, sval, update))
    except BaseException as e:
        logging.info(e)
        return


import_tm_point = PythonOperator(
    task_id='import_tm_point',
    python_callable=preInsertRowPoint,
    provide_context=True,
    dag=dag
)

import_metrics_record = PythonOperator(
    task_id='import_metrics_record',
    python_callable=metricsCount,
    provide_context=True,
    dag=dag
)


def get_driver_num(**op_kwargs):
    driver_num = {}
    res = []
    conn = get_db_conn('mysql_oride_data_readonly')
    mcursor = conn.cursor()
    driver_id = -1
    results = tuple()
    driver_dic = {}
    while True:
        mcursor.execute(query_driver_city_serv.format(id=driver_id))
        conn.commit()
        tmp = mcursor.fetchall()
        if not tmp:
            break
        results += tmp
        driver_id = tmp[-1][0]

    mcursor.close()
    conn.close()
    for data in results:
        driver_dic[data[0]] = ",".join([str(data[1]), str(data[2])])
    redis_conn = RedisHook(redis_conn_id='pika_85').get_conn()
    ts = op_kwargs['ts']
    dt, h = ts.split('T')
    dt = dt + ' ' + h.split('+')[0]
    time_array = time.strptime(dt, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(time_array))
    a_member = set()
    no_member = set()
    dt_start = time.strftime('%Y%m%d%H%M', time.localtime(timestamp))
    for i in range(0, 10):
        dt = time.strftime('%Y%m%d%H%M', time.localtime(timestamp + i * 60))
        a_member = a_member.union(set(redis_conn.smembers(active_a_driver % dt)))
        no_member = no_member.union(set(redis_conn.smembers(active_no_driver % dt)))
    for mem in a_member:
        tmp = driver_dic.get(int(mem), '-1,0')
        if tmp not in driver_num:
            driver_num[tmp] = {"a_mem": 0, "no_mem": 0}
        driver_num[tmp]["a_mem"] += 1
    for mem in no_member:
        tmp = driver_dic.get(int(mem), '-1,0')
        if tmp not in driver_num:
            driver_num[tmp] = {"a_mem": 0, "no_mem": 0}
        driver_num[tmp]["no_mem"] += 1

    for k, v in driver_num.items():
        info = k.split(",")
        res.append([int(info[0]), int(info[1]), dt_start+'00', v["a_mem"], v["no_mem"]])

    conn = get_db_conn('mysql_bi')
    mcursor = conn.cursor()
    mcursor.executemany(insert_driver_num, res)
    conn.commit()
    mcursor.close()
    conn.close()




import_driver_num = PythonOperator(
    task_id='import_driver_num',
    python_callable=get_driver_num,
    provide_context=True,
    dag=dag
)

create_oride_orders_status >> import_driver_num >> import_tm_point >> import_metrics_record
