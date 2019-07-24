# coding: utf-8
import airflow
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.hive_hooks import HiveCliHook,HiveServer2Hook
import logging

"""
根据binlog updated_at字段修复ods表数据
airflow trigger_dag oride_repair_ods_table_data  --conf '{"table_name": "data_order", "binlog_start_date":"2019-07-18", "binlog_end_date":"2019-07-23", "ods_start_date":"2019-07-18", "ods_end_date":"2019-07-23"}'
"""

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime.utcnow(),
}

dag = airflow.DAG(
    'oride_repair_ods_table_data',
    schedule_interval=None,
    default_args=args)

def run_insert_ods(**kwargs):
    col_sql='''
        DESCRIBE oride_dw.ods_binlog_{table}_hi
    '''.format(table=kwargs['dag_run'].conf['table_name'])
    logging.info('Executing: %s', col_sql)
    hive2_conn=HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    cursor.execute(col_sql)
    #print(cursor.fetchall())
    column_rows=[]
    for data in cursor.fetchall():
        if data[0]=='op' or data[0]=='ts_ms':
            column_rows.append(data[0])
        elif data[0]=='gtid':
            column_rows.append("get_json_object(source, '$.gtid')")
        elif data[0]=='pos':
            column_rows.append("get_json_object(source, '$.pos')")
        elif data[0]=='dt':
            break
        else:
            column_rows.append("get_json_object(after, '$.{}')".format(data[0]))

    print(column_rows)
    sql='''
        msck REPAIR TABLE oride_source.binlog_{table};
        SET hive.exec.dynamic.partition=true;
        SET hive.exec.dynamic.partition.mode=nonstrict;
        INSERT OVERWRITE TABLE oride_dw.`ods_binlog_{table}_hi` partition(dt, hour)
        SELECT
            {columns},
            from_unixtime(unix_timestamp(regexp_replace(get_json_object(after, '$.updated_at'), 'T', ' '))+3600, 'yyyy-MM-dd') as dt,
            from_unixtime(unix_timestamp(regexp_replace(get_json_object(after, '$.updated_at'), 'T', ' '))+3600, 'HH') as hour
        FROM
            oride_source.binlog_{table}
        WHERE
            dt BETWEEN '{b_st}' AND '{b_et}'
            AND from_unixtime(unix_timestamp(regexp_replace(get_json_object(after, '$.updated_at'), 'T', ' '))+3600, 'yyyy-MM-dd') between '{o_st}' and '{o_et}'
    '''
    hive_hook = HiveCliHook()
    run_sql=sql.format(
        table=kwargs['dag_run'].conf['table_name'],
        columns=",\n".join(column_rows),
        b_st=kwargs['dag_run'].conf['binlog_start_date'],
        b_et=kwargs['dag_run'].conf['binlog_end_date'],
        o_st=kwargs['dag_run'].conf['ods_start_date'],
        o_et=kwargs['dag_run'].conf['ods_end_date']
    )
    logging.info('Executing: %s', run_sql)
    hive_hook.run_cli(run_sql)


insert_ods = PythonOperator(
    task_id='insert_ods_data',
    provide_context=True,
    python_callable=run_insert_ods,
    dag=dag,
)

