import airflow
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from utils.connection_helper import get_db_conf
from airflow.operators.hive_operator import HiveOperator

args = {
    'owner': 'root',
    'start_date': datetime(2019, 6, 22),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_import_mysql_to_hive',
    schedule_interval="0 0 * * *",
    default_args=args)

table_list = [
    "data_driver_extend",
    "data_order",
    "data_user_extend",
]

host, port, schema, login, password = get_db_conf('sqoop_db')
for table_name in table_list:
    import_table = BashOperator(
        task_id='import_table_{}'.format(table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir ufile://opay-datalake/oride/db/{table}/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(host=host, port=port, schema=schema, username=login, password=password,table=table_name),
        dag=dag,
    )
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(table_name),
        hql='''
            ALTER TABLE oride_db.{table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}')
        '''.format(table=table_name),
        schema='oride_source',
        dag=dag)


    import_table >> add_partitions
