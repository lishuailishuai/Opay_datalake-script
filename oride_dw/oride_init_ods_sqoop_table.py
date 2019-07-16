# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveCliHook
import logging

"""
新建binlog导入表 && ods表
airflow trigger_dag oride_init_ods_sqoop_table  --conf '{"db_name": "oride_data", "table_name": "test", "conn_id":"sqoop_db"}'
"""

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime.utcnow(),
}

dag = airflow.DAG(
    'oride_init_ods_sqoop_table',
    schedule_interval=None,
    default_args=args)

ODS_DB='oride_dw'

ODS_CREATE_TABLE_SQL='''
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.`ods_sqoop_{table_name}_df`(
        {columns}
    )
    PARTITIONED BY (
      `dt` string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION
      'ufile://opay-datalake/oride/db/{table_name}';
'''

def run_create_ods_table(**kwargs):
    # get table column
    column_sql='''
        SELECT
            COLUMN_NAME,
            DATA_TYPE,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,COLUMN_COMMENT
        FROM
            information_schema.columns
        WHERE
            table_schema='{db_name}' and table_name='{table_name}'
    '''.format(db_name=kwargs['dag_run'].conf['db_name'], table_name=kwargs['dag_run'].conf['table_name'])
    mysql_hook=MySqlHook(kwargs['dag_run'].conf['conn_id'])
    mysql_conn = mysql_hook.get_conn()
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(column_sql)
    results = mysql_cursor.fetchall()
    rows=[]
    for result in results:
        if result[1]=='timestamp' or result[1]=='varchar' or result[1]=='char' or result[1]=='text' or result[1]=='datetime':
            data_type='string'
        elif result[1]=='decimal':
            data_type=result[1]+"("+str(result[2]) + "," + str(result[3])+")"
        else:
            data_type=result[1]
        rows.append("`%s` %s comment '%s'" % (result[0], data_type, result[4]))
    mysql_conn.close()

    # hive create table
    hive_hook = HiveCliHook()
    sql=ODS_CREATE_TABLE_SQL.format(
        db_name=ODS_DB,
        table_name=kwargs['dag_run'].conf['table_name'],
        columns=",\n".join(rows)
    )
    logging.info('Executing: %s', sql)
    hive_hook.run_cli(sql)

create_ods_table = PythonOperator(
    task_id='create_ods_table',
    provide_context=True,
    python_callable=run_create_ods_table,
    dag=dag,
)
