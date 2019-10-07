import airflow
from datetime import datetime, timedelta
from airflow.operators.hive_operator import HiveOperator
from airflow.hooks.hive_hooks import HiveCliHook,HiveServer2Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.mysql_hook import MySqlHook
from plugins.comwx import ComwxApi
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException
from airflow.operators.bash_operator import BashOperator
import time
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

ODS_DB='oride_dw_ods'
BUCKET_NAME='opay-bi'

def run_insert_ods(ds, execution_date, **kwargs):
    col_sql='''
        DESCRIBE {hive_db}.ods_binlog_{table}_hi
    '''.format(hive_db=ODS_DB, table=kwargs["params"]["table"])

    hive2_conn=HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    logging.info('Executing: %s', col_sql)
    cursor.execute(col_sql)
    hive_table_columns=[]
    for data in cursor.fetchall():
        if data[0]=='dt':
            break;
        if data[0] not in ['op', 'ts_ms', 'gtid', 'pos']:
            hive_table_columns.append(data[0])
    cursor.close()

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
    '''.format(db_name='oride_data', table_name=kwargs["params"]["table"])
    mysql_hook=MySqlHook('sqoop_db')
    mysql_conn = mysql_hook.get_conn()
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(column_sql)
    results = mysql_cursor.fetchall()
    add_columns=[]
    for result in results:
        if result[1]=='timestamp' or result[1]=='varchar' or result[1]=='char' or result[1]=='text':
            data_type='string'
        elif result[1]=='decimal':
            data_type=result[1]+"("+str(result[2]) + "," + str(result[3])+")"
        else:
            data_type=result[1]
        if result[0] not in hive_table_columns:
            add_columns.append("`%s` %s comment '%s'" % (result[0], data_type, result[4]))
            hive_table_columns.append(result[0])
    mysql_conn.close()
    hive_hook = HiveCliHook()
    if len(add_columns)>0:
        add_columns_sql='''
            ALTER TABLE {hive_db}.`ods_binlog_{table}_hi` add columns ({columns})

        '''.format(hive_db=ODS_DB, table=kwargs["params"]["table"], columns=",".join(add_columns))
        logging.info('Executing: %s', add_columns_sql)
        hive_hook.run_cli(add_columns_sql)
    column_rows=[
        'op',
        'ts_ms',
        "get_json_object(source, '$.gtid')",
        "get_json_object(source, '$.pos')"

    ]
    for col_name in hive_table_columns:
        column_rows.append("get_json_object(after, '$.{}')".format(col_name))

    sql='''
        INSERT OVERWRITE TABLE {hive_db}.`ods_binlog_{table}_hi` partition(dt='{ds}', hour='{hour}')
        SELECT
            {columns}
        FROM
            oride_source.binlog_{table}
        WHERE
            dt='{ds}' AND hour='{hour}'
    '''
    run_sql=sql.format(hive_db=ODS_DB, table=kwargs["params"]["table"],ds=ds,hour=execution_date.strftime("%H"), columns=",\n".join(column_rows))
    logging.info('Executing: %s', run_sql)
    hive_hook.run_cli(run_sql)


BINLOG_TABLE_LIST_VAR_NAME='oride_binlog_table_list'
binlog_table_list=Variable.get(BINLOG_TABLE_LIST_VAR_NAME) if Variable.get(BINLOG_TABLE_LIST_VAR_NAME) is not None else ''

def check_s3_prefix(ds, execution_date, **kwargs):
    table=kwargs["params"]["table"]
    hour=execution_date.strftime("%H")
    hook = S3Hook(aws_conn_id='aws_default', verify=None)
    prefix='oride_binlog/oride_binlog.oride_data.{table}/dt={ds}/hour={hour}'.format(
        table=table,
        ds=ds,
        hour=hour)
    poke_interval=30
    try_max_num=10
    num=1
    while not hook.check_for_prefix(prefix=prefix, delimiter='/', bucket_name=BUCKET_NAME):
        logging.info('Check s3 prefix : %s in bucket s3://%s', prefix, BUCKET_NAME)
        if num >= try_max_num:
            comwx = ComwxApi('wwd26d45f97ea74ad2', 'BLE_v25zCmnZaFUgum93j3zVBDK-DjtRkLisI_Wns4g', '1000011')
            comwx.postAppMessage('oride binlog 数据采集，table:{0} date:{1} hour:{2} 数据记录为0，请及时排查，谢谢'.format(table, ds, hour), '271')
            raise AirflowException("check s3 prefix failed!")
        else:
            time.sleep(poke_interval)
            num+=1
    logging.info("Success criteria met. Exiting.")


HDFS_PATH = "/user/hive/warehouse/oride_dw_ods.db/ods_binlog_{table}_hi/dt={dt}/hour={hour}"
IGNORE_TABLE_LIST = [
    'data_driver'
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
        touchz_data_success = BashOperator(
            task_id='touchz_data_success_{}'.format(table),
            bash_command="""
                line_num=`$HADOOP_HOME/bin/hadoop fs -du -s {hdfs_data_dir} | tail -1 | awk '{{print $1}}'`

                if [ $line_num -eq 0 ]
                then
                    echo "FATAL {hdfs_data_dir} is empty"
                    exit 1
                else
                    echo "DATA EXPORT Successed ......"
                    $HADOOP_HOME/bin/hadoop fs -touchz {hdfs_data_dir}/_SUCCESS
                fi
            """.format(
                hdfs_data_dir=HDFS_PATH.format(table=table, dt='{{ds}}', hour='{{execution_date.strftime("%H")}}')
            ),
            dag=dag)
        if table not in IGNORE_TABLE_LIST:
            check_file = PythonOperator(
                task_id='check_file_{}'.format(table),
                provide_context=True,
                python_callable=check_s3_prefix,
                params={'table':table},
                dag=dag,
            )
            check_file >> binlog_add_partitions
        binlog_add_partitions >> insert_ods
        insert_ods >> touchz_data_success
