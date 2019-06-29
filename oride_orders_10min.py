'''
add by duo.wu 下单量、下单人数、接单量、在线司机数，从业务从库读取数据
'''

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import timedelta, time, datetime
from utils.connection_helper import get_db_conf
import os

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

create_oride_orders_status >> write_from_mysql
