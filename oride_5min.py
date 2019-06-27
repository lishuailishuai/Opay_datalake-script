'''
add by duo.wu 小时下单数、下单人数、接单数
'''

import airflow
from airflow.operators.impala_plugin import ImpalaOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, time, datetime
from utils.connection_helper import get_db_conf

args = {
    'owner': 'root',
    'start_date': datetime(2019, 5, 11),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_5min',
    schedule_interval="*/2 * * * *",
    default_args=args
)

create_driver_status = HiveOperator(
    task_id='create_driver_status',
    hql="""
        CREATE TABLE IF NOT EXISTS oride_driver_status (
            create_at string,
            driver_id bigint,
            serv_model int,
            serv_status int
        )
        PARTITIONED BY (`dt` string)
        STORED AS TEXTFILE;
    """,
    schema='dashboard',
    dag=dag
)

add_partitions = HiveOperator(
    task_id='add_partitions',
    hql="""
        ALTER TABLE oride_driver_status ADD IF NOT EXISTS PARTITION (dt = '{{ ds }}');
    """,
    schema='dashboard',
    dag=dag
)

host, port, schema, login, password = get_db_conf('sqoop_db')
write_from_mysql = BashOperator(
    task_id='write_from_mysql',
    bash_command='''
        #!/usr/bin/env bash
        sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
        --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
        --username {username} \
        --password {password} \
        --query 'select now(), id, serv_mode, serv_status from data_driver_extend where 1 and $CONDITIONS' \
        --hive-import \
        --hive-database dashboard \
        --hive-table oride_driver_status \
        --target-dir '/tmp/oride_driver_status' \
        --num-mappers 1 \
        --hive-partition-key 'dt' \
        --hive-partition-value '{{{{ ds }}}}'
    '''.format(host=host, port=port, schema=schema, username=login, password=password),
    dag=dag,
)

refresh_impala = ImpalaOperator(
    task_id='refresh_impala',
    hql="""\
        REFRESH oride_driver_status;
    """,
    schema='dashboard',
    priority_weight=50,
    dag=dag
)

create_driver_status >> write_from_mysql >> add_partitions >> refresh_impala
