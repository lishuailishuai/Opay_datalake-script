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

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 11),
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_orders_10min',
    schedule_interval="*/10 * * * *",
    default_args=args
)

active_a_driver = "active_driver:a:%s"
active_no_driver = "active_driver:no:%s"

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
            order_time timestamp not null default '1970-01-02 00:00:00' comment 'time 10min',
            daily timestamp not null default '1970-01-02 00:00:00' comment 'time day',
            orders int unsigned not null default 0 comment 'orders',
            orders_user int unsigned not null default 0 comment 'users',
            orders_pick int unsigned not null default 0 comment 'picks',
            drivers_serv int unsigned not null default 0 comment 'drivers',
            orders_finish int unsigned not null default 0 comment 'finish',
            avg_pick int unsigned not null default 0 comment '(picktime-ordertime)/picks sec',
            avg_take decimal(10,1) unsigned not null default '0.0' comment '(taktime-picktime)/finish min',
            not_sys_cancel_orders int unsigned not null default 0 comment 'status = 6 and driver_id > 0 and cancel_role <> 3 and cancel_role <> 4',
            picked_orders int unsigned not null default 0 comment 'pickup_time > 0',
            orders_accept int unsigned not null default 0 comment 'take_time > 0',
            agg_orders_finish int unsigned not null default 0 comment '',
            primary key (order_time)
        )engine=innodb;
    """,
    database='bi',
    mysql_conn_id='mysql_bi',
    dag=dag
)

host_bi, port_bi, schema_bi, user_bi, pass_bi = get_db_conf('mysql_bi')
host, port, schema, login, password = get_db_conf('sqoop_db')
write_from_mysql = BashOperator(
    task_id='write_from_mysql',
    bash_command='''
        cd {{ params.path }}; \
        sh oride_orders_10min.sh {{ params.host }} {{ params.port }} {{ params.username }} {{ params.password }} {{ params.host_bi }} {{ params.port_bi }} {{ params.user_bi }} {{ params.pass_bi }} 
    ''',
    params={'host': host, 'port': port, 'username': login, 'password': password, 'host_bi': host_bi, 'user_bi': user_bi, 'port_bi': port_bi, 'pass_bi': pass_bi, 'path': os.path.split(os.path.realpath(__file__))[0]},
    dag=dag,
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
    redis_conn = RedisHook(redis_conn_id='pika').get_conn()
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
        tmp = driver_dic[int(mem)]
        if tmp not in driver_num:
            driver_num[tmp] = {"a_mem": 0, "no_mem": 0}
        driver_num[tmp]["a_mem"] += 1
    for mem in no_member:
        tmp = driver_dic[int(mem)]
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

create_oride_orders_status >> write_from_mysql