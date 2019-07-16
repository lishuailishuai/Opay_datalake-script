# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from airflow.hooks.mysql_hook import MySqlHook
from airflow.hooks.hive_hooks import HiveCliHook
from airflow.models import Variable

"""
新建binlog导入表 && ods表
airflow trigger_dag oride_init_ods_table  --conf '{"db_name": "oride_data", "table_name": "test"}'
"""

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime.utcnow(),
}

dag = airflow.DAG(
    'oride_init_ods_table',
    schedule_interval=None,
    default_args=args)

BINLOG_DB='oride_source'
ODS_DB='oride_dw'
BINLOG_TABLE_LIST_VAR_NAME='oride_binlog_table_list'

BINLOG_CREATE_TABLE_SQL='''
    CREATE EXTERNAL TABLE IF NOT EXISTS `binlog_{{ dag_run.conf["table_name"] }}`(
        `before` string,
        `after` string,
        `op` string,
        `ts_ms` bigint,
        `source` string
    )
    PARTITIONED BY (
        `dt` string,
        `hour` string)
    ROW FORMAT SERDE
        'org.openx.data.jsonserde.JsonSerDe'
    WITH SERDEPROPERTIES (
        'ignore.malformed.json'='true')
    LOCATION
        'hdfs://10.52.17.84/topics/fullfillment.{{ dag_run.conf["db_name"] }}.{{ dag_run.conf["table_name"] }}'
'''
ODS_CREATE_TABLE_SQL='''
CREATE TABLE IF NOT EXISTS {db_name}.`ods_binlog_{table_name}_hi`(
    `op` string comment '操作类型 c 创建 u 更新 d 删除',
    `ts_ms` bigint comment '事件时间（毫秒）',
    `gtid` string comment '事件唯一标识',
    {columns}
)
PARTITIONED BY (
    `dt` string,
    `hour` string
)
STORED AS PARQUET
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
    mysql_hook=MySqlHook('sqoop_db')
    mysql_conn = mysql_hook.get_conn()
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(column_sql)
    results = mysql_cursor.fetchall()
    rows=[]
    for result in results:
        if result[1]=='timestamp' or result[1]=='varchar' or result[1]=='char' or result[1]=='text':
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
    hive_hook.run_cli(sql)
    # write airflow var
    binlog_table_list=Variable.get(BINLOG_TABLE_LIST_VAR_NAME) if Variable.get(BINLOG_TABLE_LIST_VAR_NAME) is not None else ''
    print(binlog_table_list)
    new_binlog_table_set=set(binlog_table_list.split())
    new_binlog_table_set.add(kwargs['dag_run'].conf['table_name'])
    print(new_binlog_table_set)
    Variable.set(BINLOG_TABLE_LIST_VAR_NAME, "\n".join(list(new_binlog_table_set)))

create_ods_table = PythonOperator(
    task_id='create_ods_table',
    provide_context=True,
    python_callable=run_create_ods_table,
    dag=dag,
)


create_binlog_table = HiveOperator(
    task_id='create_binlog_table',
    hql=BINLOG_CREATE_TABLE_SQL,
    schema=BINLOG_DB,
    dag=dag)
