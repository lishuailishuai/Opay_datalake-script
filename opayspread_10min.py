"""
create table if not exists opayspread_10min (
    daily timestamp not null default '1970-01-02 00:00:00' comment '日期',
    city varchar(32) not null default '' comment '城市名',
    group_name varchar(100) not null default '' comment '协会名',
    team_id int not null default -1 comment '司官ID',
    team_name varchar(100) not null default '' comment '司官名',
    drivers int unsigned not null default 0 comment '司机数',
    teams int unsigned not null default 0 comment '司官数',
    ordertakes int unsigned not null default 0 comment '接单数',
    drivertakes int unsigned not null default 0 comment '接单骑手数',
    orderfinishs int unsigned not null default 0 comment '完成支付订单数',
    driverfinishs int unsigned not null default 0 comment '完成支付骑手数',
    orderarrives int unsigned not null default 0 comment '完成订单数',
    driverarrives int unsigned not null default 0 comment '完成订单骑手数',
    driver5arrives int unsigned not null default 0 comment '完成5单骑手数',
    udt_at timestamp not null default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP comment '更新时间',
    unique index (daily, city, group_name, team_id)
)engine=innodb default charset=utf8
"""

import airflow
from plugins.comwx import ComwxApi
import time
import math
from datetime import datetime, timedelta
from utils.connection_helper import get_db_conn
from airflow.operators.python_operator import PythonOperator
import logging
import pandas
from collections import OrderedDict

args = {
    'owner': 'wuduo',
    'start_date': datetime(2019, 7, 18),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'opayspread_10min',
    schedule_interval="*/10 * * * *",
    concurrency=5,
    max_active_runs=1,
    default_args=args
)

"""
@:return dict
"""
def __getcityList():
    try:
        mysql_conn = get_db_conn('sqoop_db')
        oride_db = mysql_conn.cursor()
        msql = '''
            select id, name from data_city_conf
        '''
        oride_db.execute(msql)
        results = oride_db.fetchall()
        citys = {}
        for (city_id, city_name) in results:
            citys[city_id] = city_name

        oride_db.close()
        mysql_conn.close()
        return citys
    except BaseException as e:
        logging.info(e)
        return {}


"""
获取当天业务订单数据
@:param st 开始时间戳
@:param ed 结束时间戳
@:return pandas.DataFrame
"""
def __getOrideOrders(st, ed):
    try:
        mysql_conn = get_db_conn('sqoop_db')
        oride_db = mysql_conn.cursor()
        msql = '''
            select 
                driver_id,  
                count(distinct if(take_time>={st} and take_time<{ed}, id, null)) as ordertakes,
                count(distinct if(finish_time>={st} and finish_time<{ed}, id, null)) as orderfinishs,
                count(distinct if(arrive_time>={st} and arrive_time<{ed}, id, null)) as orderarrives,
                if(count(distinct if(take_time>={st} and take_time<{ed}, id, null))>0, 1, 0) as drivertakes,
                if(count(distinct if(finish_time>={st} and finish_time<{ed}, id, null))>0, 1, 0) as driverfinishs,
                if(count(distinct if(arrive_time>={st} and arrive_time<{ed}, id, null))>0, 1, 0) as driverarrives,
                if(count(distinct if(arrive_time>={st} and arrive_time<{ed}, id, null))>=5, 1, 0) as driver5arrives 
            from data_order 
            where ((arrive_time >= {st} and arrive_time < {ed}) or 
                (take_time >= {st} and take_time < {ed})) and 
                driver_serv_type = 2  
            group by driver_id 
        '''.format(
            st=st,
            ed=ed
        )
        logging.info(msql)
        oride_db.execute("set time_zone = '+1:00'")
        oride_db.execute(msql)
        results = oride_db.fetchall()
        driver_info = {
            'driver_id': [],
            'ordertakes': [],
            'orderfinishs': [],
            'orderarrives': [],
            'drivertakes': [],
            'driverfinishs': [],
            'driverarrives': [],
            'driver5arrives': []
        }
        for (driver_id, ordertakes, orderfinishs, orderarrives, drivertakes, driverfinishs, driverarrives, driver5arrives) in results:
            driver_info['driver_id'].append(driver_id)
            driver_info['ordertakes'].append(int(ordertakes))
            driver_info['orderfinishs'].append(int(orderfinishs))
            driver_info['orderarrives'].append(int(orderarrives))
            driver_info['drivertakes'].append(int(drivertakes))
            driver_info['driverfinishs'].append(int(driverfinishs))
            driver_info['driverarrives'].append(int(driverarrives))
            driver_info['driver5arrives'].append(int(driver5arrives))

        #logging.info(pandas.DataFrame(driver_info))
        oride_db.close()
        mysql_conn.close()
        return pandas.DataFrame(driver_info)
    except BaseException as e:
        logging.info(e)
        return None


"""
获取当天协会等数据
@:return pandas.DataFrame
"""
def __getOpaySpreadDrivers():
    try:
        citys = __getcityList()
        logging.info(citys)
        mysql_conn = get_db_conn('opay_spread_mysql')
        spread_db = mysql_conn.cursor()
        msql = '''
            select 
                min(if(isnull(gt.team_id), 0, gt.team_id)), 
                min(if(isnull(gt.city), 0, gt.city)), 
                min(if(isnull(gt.team_name), 'other', gt.team_name)), 
                min(if(isnull(gt.group_name), 'other', gt.group_name)), 
                r.driver_id 
            from 
                rider_signups r left join 
                (select 
                    t.id as team_id,
                    t.city,
                    t.name as team_name,
                    g.name as group_name 
                from driver_group g left join driver_team t 
                on g.id = t.group_id 
                where g.del = 0 
                ) gt 
            on gt.team_id = r.team_id 
            where r.driver_id > 0 
            group by r.driver_id 
        '''
        #-- where g.del = 0 and t.del = 0
        logging.info(msql)
        spread_db.execute(msql)
        results = spread_db.fetchall()
        #logging.info(results)
        group_info = {
            'team_id': [],
            'city': [],
            'team_name': [],
            'group_name': [],
            'driver_id': []
        }
        for (team_id, city, team_name, group_name, driver_id) in results:
            group_info['team_id'].append(team_id)
            group_info['city'].append(citys.get(int(city), 'other'))
            group_info['team_name'].append(team_name)
            group_info['group_name'].append(group_name)
            group_info['driver_id'].append(driver_id)

        #logging.info(pandas.DataFrame(group_info))
        spread_db.close()
        mysql_conn.close()
        return pandas.DataFrame(group_info)
    except BaseException as e:
        logging.info(e)
        return None


"""
10分钟统计入口
@:param op_kwargs
"""
def opayspreadCount(**op_kwargs):
    test_mode = op_kwargs.get('test_mode', False)
    if test_mode:
        ds = op_kwargs.get('ds', time.strftime('%Y-%m-%d', time.localtime(time.time())))
        prev_day_start = int(time.mktime(datetime.strptime(ds, '%Y-%m-%d').timetuple()))
    else:
        prev_timepoint = math.floor(int(time.time()) / 600) * 600 - 600
        prev_day_start = math.floor(prev_timepoint/86400) * 86400
    prev_day_end = prev_day_start + 86400

    bidbconn = get_db_conn('mysql_bi')
    bidb = bidbconn.cursor()

    driver_orders = __getOrideOrders(prev_day_start, prev_day_end)
    driver_framework = __getOpaySpreadDrivers()

    if driver_orders is None or driver_framework is None:
        raise ValueError('get orders or groups error')

    #results = pandas.merge(driver_orders, driver_framework, on='driver_id')
    results = driver_framework.merge(driver_orders, how='left', on=['driver_id'])
    #logging.info(results.tail)
    #按城市、group汇总数据
    group_results = results.groupby(['city', 'group_name']).agg(
        OrderedDict([
            ('city', 'min'),
            ('group_name', 'min'),
            ('driver_id', 'count'),
            ('team_id', 'nunique'),
            ('ordertakes', 'sum'),
            ('orderfinishs', 'sum'),
            ('orderarrives', 'sum'),
            ('drivertakes', 'sum'),
            ('driverfinishs', 'sum'),
            ('driverarrives', 'sum'),
            ('driver5arrives', 'sum')
        ])
    )
    #logging.info(group_results)
    #保存结果到数据库
    __dataToMysql(
        time.strftime('%Y-%m-%d 00:00:00', time.localtime(prev_day_start)),
        bidb,
        group_results.values.tolist(),
        ['daily', 'city', 'group_name', 'drivers', 'teams', 'ordertakes', 'orderfinishs',
         'orderarrives', 'drivertakes', 'driverfinishs', 'driverarrives', 'driver5arrives'],
        '''
        teams=values(teams), drivers=values(drivers), ordertakes=values(ordertakes), orderfinishs=values(orderfinishs), 
        orderarrives=values(orderarrives), drivertakes=values(drivertakes), driverfinishs=values(driverfinishs), 
        driverarrives = values(driverarrives), driver5arrives=values(driver5arrives)
        '''
    )

    team_results = results.groupby(['city', 'group_name', 'team_id']).agg(
        OrderedDict([
            ('city', 'min'),
            ('group_name', 'min'),
            ('team_id', 'min'),
            ('team_name', 'max'),
            ('driver_id', 'count'),
            ('ordertakes', 'sum'),
            ('orderfinishs', 'sum'),
            ('orderarrives', 'sum'),
            ('drivertakes', 'sum'),
            ('driverfinishs', 'sum'),
            ('driverarrives', 'sum'),
            ('driver5arrives', 'sum')
        ])
    )
    #logging.info(team_results)
    #保存结果到数据库
    __dataToMysql(
        time.strftime('%Y-%m-%d 00:00:00', time.localtime(prev_day_start)),
        bidb,
        team_results.values.tolist(),
        ['daily', 'city', 'group_name', 'team_id', 'team_name', 'drivers', 'ordertakes', 'orderfinishs',
         'orderarrives', 'drivertakes', 'driverfinishs', 'driverarrives', 'driver5arrives'],
        '''
        drivers=values(drivers), ordertakes=values(ordertakes), orderfinishs=values(orderfinishs), 
        orderarrives=values(orderarrives), drivertakes=values(drivertakes), driverfinishs=values(driverfinishs), 
        driverarrives = values(driverarrives), driver5arrives=values(driver5arrives)
        '''
    )

    bidbconn.close()


def __dataToMysql(daily, conn, data, column, update):
    isql = 'insert into opayspread_10min ({})'.format(','.join(column))
    val = ''
    cnt = 0
    try:
        for row in data:
            if val == '':
                val = '(\'{0}\', \'{1}\')'.format(daily, '\',\''.join([str(x).replace("'", "\\'") for x in row]))
            else:
                val += ',(\'{0}\', \'{1}\')'.format(daily, '\',\''.join([str(x).replace("'", "\\'") for x in row]))
            cnt += 1

            if cnt >= 1000:
                #logging.info('{0} values {1} on duplicate key update {2}'.format(isql, val, update))
                conn.execute('{0} values {1} on duplicate key update {2}'.format(isql, val, update))
                cnt = 0
                val = ''

        if cnt > 0 and val != '':
            #logging.info('{0} values {1} on duplicate key update {2}'.format(isql, val, update))
            conn.execute('{0} values {1} on duplicate key update {2}'.format(isql, val, update))
    except BaseException as e:
        logging.info(e)
        return


opayspread_count_task = PythonOperator(
    task_id='opayspread_count_task',
    python_callable=opayspreadCount,
    provide_context=True,
    dag=dag
)

opayspread_count_task
