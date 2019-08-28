import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.hooks.hive_hooks import HiveCliHook,HiveServer2Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.s3_prefix_sensor import S3PrefixSensor
import logging

args = {
    'owner': 'zhenqian.zhang',
    'start_date': datetime(2019, 7, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = airflow.DAG(
    'oride_source_binlog',
    schedule_interval="05 * * * *",
    default_args=args)


def run_insert_ods(ds, execution_date, **kwargs):
    col_sql='''
        DESCRIBE oride_dw.ods_binlog_{table}_hi
    '''.format(table=kwargs["params"]["table"])

    print(col_sql)
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
        INSERT OVERWRITE TABLE oride_dw.`ods_binlog_{table}_hi` partition(dt='{ds}', hour='{hour}')
        SELECT
            {columns}
        FROM
            oride_source.binlog_{table}
        WHERE
            dt='{ds}' AND hour='{hour}'
    '''
    hive_hook = HiveCliHook()
    run_sql=sql.format(table=kwargs["params"]["table"],ds=ds,hour=execution_date.strftime("%H"), columns=",\n".join(column_rows))
    logging.info('Executing: %s', run_sql)
    hive_hook.run_cli(run_sql)


BINLOG_TABLE_LIST_VAR_NAME='oride_binlog_table_list'
binlog_table_list=Variable.get(BINLOG_TABLE_LIST_VAR_NAME) if Variable.get(BINLOG_TABLE_LIST_VAR_NAME) is not None else ''

table_check_list = [
    "data_order",
    "data_order_payment"
]
if binlog_table_list!='':
    for table in binlog_table_list.split():
        binlog_add_partitions = HiveOperator(
            task_id='binlog_add_partitions_{}'.format(table),
            hql="""
            ALTER TABLE binlog_{table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}', hour = '{{{{ execution_date.strftime("%H") }}}}');
        """.format(table=table),
            schema='oride_source',
            dag=dag)

        insert_ods = PythonOperator(
            task_id='insert_ods_{}'.format(table),
            provide_context=True,
            python_callable=run_insert_ods,
            params={'table':table},
            dag=dag,
        )
        if table in table_check_list:
            check_file = S3PrefixSensor(
                task_id='check_file_{}'.format(table),
                prefix='oride_binlog/oride_binlog.oride_data.{table}/dt={{{{ ds }}}}/hour={{{{ execution_date.strftime("%H") }}}}'.format(table=table),
                bucket_name='opay-bi',
                timeout=3600,
                dag=dag)

            check_file >> binlog_add_partitions

        binlog_add_partitions >> insert_ods
