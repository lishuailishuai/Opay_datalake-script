# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.hive_hooks import HiveCliHook,HiveServer2Hook
import logging

"""
db 历史数据写入 ods binlog
airflow trigger_dag oride_db_history_to_ods  --conf '{"table_name": "data_order", "end_date":"2019-07-17"}'
"""

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime.utcnow(),
}

dag = airflow.DAG(
    'oride_db_history_to_ods',
    schedule_interval=None,
    concurrency=1,
    max_active_runs=1,
    default_args=args)

ODS_DB='oride_dw_ods'
def run_insert_ods(**kwargs):
    hive2_conn=HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    # 获取时间列表
    get_date_sql='''
        SELECT
            distinct from_unixtime(unix_timestamp(substr(updated_at, 1, 19), 'yyyy-MM-dd'), 'yyyy-MM-dd') as ut
        FROM
            {db}.ods_sqoop_base_{table}_df
        WHERE
            dt='{end_date}' and updated_at<='{end_date} 23:59:59'
        ORDER BY ut ASC
    '''.format(db=ODS_DB, table=kwargs['dag_run'].conf['table_name'], end_date=kwargs['dag_run'].conf['end_date'])
    logging.info('Executing: %s', get_date_sql)
    cursor.execute(get_date_sql)
    date_list=cursor.fetchall()
    col_sql='''
        DESCRIBE {db}.ods_binlog_{table}_hi
    '''.format(db=ODS_DB, table=kwargs['dag_run'].conf['table_name'])
    logging.info('Executing: %s', col_sql)
    cursor.execute(col_sql)
    col_list=cursor.fetchall()
    column_rows=[]
    for data in col_list:
        if data[0]=='op':
            column_rows.append(" NULL as op")
        elif data[0]=='ts_ms':
            column_rows.append(" 0 as ts_ms")
        elif data[0]=='gtid':
            column_rows.append(" NULL as gtid")
        elif data[0]=='pos':
            column_rows.append(" 0 as pos")
        elif data[0]=='dt':
            break
        else:
            column_rows.append(" {} ".format(data[0]))

    for dt in date_list:
        sql='''
            SET hive.exec.dynamic.partition=true;
            SET hive.exec.dynamic.partition.mode=nonstrict;
            INSERT OVERWRITE TABLE {db}.`ods_binlog_{table}_hi` partition(dt, hour)
            SELECT
                {columns},
                from_unixtime(unix_timestamp(substr(updated_at, 1, 19), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd') as dt,
                from_unixtime(unix_timestamp(substr(updated_at, 1, 19), 'yyyy-MM-dd HH:mm:ss'), 'HH') as hour
            FROM
                {db}.ods_sqoop_base_{table}_df
            WHERE
                dt='{end_date}'
                AND from_unixtime(unix_timestamp(substr(updated_at, 1, 19), 'yyyy-MM-dd HH:mm:ss'), 'yyyy-MM-dd')='{dt}'
        '''
        hive_hook = HiveCliHook()
        run_sql=sql.format(
            db=ODS_DB,
            table=kwargs['dag_run'].conf['table_name'],
            columns=",\n".join(column_rows),
            end_date=kwargs['dag_run'].conf['end_date'],
            dt=dt[0]
        )
        logging.info('Executing: %s', run_sql)
        hive_hook.run_cli(run_sql)

insert_ods = PythonOperator(
    task_id='insert_ods_data',
    provide_context=True,
    python_callable=run_insert_ods,
    dag=dag,
)

