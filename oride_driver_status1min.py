'''
add by duo.wu 每分钟扫描业务从库司机状态全表
'''

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import timedelta, datetime
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
    'oride_driver_status1min',
    schedule_interval="*/1 * * * *",
    default_args=args
)

create_oride_driver_status = MySqlOperator(
    task_id='create_oride_driver_status',
    sql="""
        CREATE TABLE IF NOT EXISTS oride_driver_status_1min (
            create_time timestamp not null default '1970-01-02 00:00:00' comment 'time 1min',
            daily timestamp not null default '1970-01-02 00:00:00' comment 'time day',
            driver_id bigint unsigned not null default 0 comment 'driver',
            serv_model int unsigned not null default 0 comment 'serv_model',
            serv_status int unsigned not null default 0 comment 'serv_status',
            city int unsigned not null default 0 comment 'city',
            key (create_time)
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
        sh oride_driver_status1min.sh {{ params.host }} {{ params.port }} {{ params.username }} {{ params.password }} {{ params.host_bi }} {{ params.port_bi }} {{ params.user_bi }} {{ params.pass_bi }} 
    ''',
    params={'host': host, 'port': port, 'username': login, 'password': password, 'host_bi': host_bi, 'user_bi': user_bi, 'port_bi': port_bi, 'pass_bi': pass_bi, 'path': os.path.split(os.path.realpath(__file__))[0]},
    dag=dag,
)

create_oride_driver_status >> write_from_mysql
