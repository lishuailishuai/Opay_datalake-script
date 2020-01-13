import airflow
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.hive_hooks import HiveCliHook, HiveServer2Hook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.hive_operator import HiveOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.sql_sensor import SqlSensor
from datetime import datetime, timedelta
import logging
from plugins.SqoopSchemaUpdate import SqoopSchemaUpdate
from plugins.TaskTimeoutMonitor import TaskTimeoutMonitor
from utils.util import on_success_callback
from utils.validate_metrics_utils import *
from airflow.models import Variable

args = {
    'owner': 'linan',
    'start_date': datetime(2019, 10, 30),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['bigdata_dw@opay-inc.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'on_success_callback':on_success_callback,
}

schedule_interval="00 01 * * *"

dag = airflow.DAG(
    'opos_source_sqoop_df',
    schedule_interval=schedule_interval,
    concurrency=10,
    max_active_runs=1,
    default_args=args)

dag_monitor = airflow.DAG(
    'opos_source_sqoop_df_monitor',
    schedule_interval=schedule_interval,
    default_args=args)

##----------------------------------------- 任务超时监控 ---------------------------------------##

def fun_task_timeout_monitor(ds, db_name, table_name, **op_kwargs):
    tb = [
        {"db": db_name, "table":table_name, "partition": "dt={pt}".format(pt=ds), "timeout": "7200"}
    ]

    TaskTimeoutMonitor().set_task_monitor(tb)



'''
导入数据的列表
db_name,table_name,conn_id,prefix_name
'''
#

table_list = [

    # crm数据
    ("opay_crm", "bd_shop", "opos_opay_crm", "base"),
    ("opay_crm", "bd_city", "opos_opay_crm", "base"),
    ("opay_crm", "bd_admin_users", "opos_opay_crm", "base"),

]
HIVE_DB = 'opos_dw_ods'
HIVE_TABLE = 'ods_sqoop_%s_%s_df'
UFILE_PATH = 'oss://opay-datalake/opos_dw_sqoop/%s/%s'
ODS_CREATE_TABLE_SQL = '''
    CREATE EXTERNAL TABLE IF NOT EXISTS {db_name}.`{table_name}`(
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
      '{ufile_path}';
    MSCK REPAIR TABLE {db_name}.`{table_name}`;
'''


def run_check_table(db_name, table_name, conn_id, hive_table_name, **kwargs):
    sqoopSchema = SqoopSchemaUpdate()
    response = sqoopSchema.update_hive_schema(
        hive_db=HIVE_DB,
        hive_table=hive_table_name,
        mysql_db=db_name,
        mysql_table=table_name,
        mysql_conn=conn_id
    )
    if response:
        return True

    # SHOW TABLES in oride_db LIKE 'data_aa'
    check_sql = 'SHOW TABLES in %s LIKE \'%s\'' % (HIVE_DB, hive_table_name)
    hive2_conn = HiveServer2Hook().get_conn()
    cursor = hive2_conn.cursor()
    cursor.execute(check_sql)
    if len(cursor.fetchall()) == 0:
        logging.info('Create Hive Table: %s.%s', HIVE_DB, hive_table_name)
        # get table column
        column_sql = '''
            SELECT
                COLUMN_NAME,
                DATA_TYPE,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,COLUMN_COMMENT
            FROM
                information_schema.columns
            WHERE
                table_schema='{db_name}' and table_name='{table_name}'
        '''.format(db_name=db_name, table_name=table_name)
        mysql_hook = MySqlHook(conn_id)
        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute(column_sql)
        results = mysql_cursor.fetchall()
        rows = []
        for result in results:
            if result[0] == 'dt':
                col_name = '_dt'
            else:
                col_name = result[0]
            if result[1] == 'timestamp' or result[1] == 'varchar' or result[1] == 'char' or result[1] == 'text' or \
                    result[1] == 'datetime' or result[1] == 'mediumtext' or result[1] == 'enum' or result[1] == 'longtext':
                data_type = 'string'
            elif result[1] == 'decimal':
                data_type = result[1] + "(" + str(result[2]) + "," + str(result[3]) + ")"
            elif result[1] == 'mediumint':
                data_type = 'int'
            else:
                data_type = result[1]
            rows.append("`%s` %s comment '%s'" % (col_name, data_type, result[4]))
        mysql_conn.close()

        # hive create table
        hive_hook = HiveCliHook()
        sql = ODS_CREATE_TABLE_SQL.format(
            db_name=HIVE_DB,
            table_name=hive_table_name,
            columns=",\n".join(rows),
            ufile_path=UFILE_PATH % (db_name, table_name)
        )
        logging.info('Executing: %s', sql)
        hive_hook.run_cli(sql)
    return


conn_conf_dict = {}
for db_name, table_name, conn_id, prefix_name in table_list:
    if conn_id not in conn_conf_dict:
        conn_conf_dict[conn_id] = BaseHook.get_connection(conn_id)

    hive_table_name = HIVE_TABLE % (prefix_name, table_name)
    # sqoop import
    import_table = BashOperator(
        task_id='import_table_{}'.format(hive_table_name),
        bash_command='''
            #!/usr/bin/env bash
            sqoop import "-Dorg.apache.sqoop.splitter.allow_text_splitter=true" \
            -D mapred.job.queue.name=root.collects \
            --connect "jdbc:mysql://{host}:{port}/{schema}?tinyInt1isBit=false&useUnicode=true&characterEncoding=utf8" \
            --username {username} \
            --password {password} \
            --table {table} \
            --target-dir {ufile_path}/dt={{{{ ds }}}}/ \
            --fields-terminated-by "\\001" \
            --lines-terminated-by "\\n" \
            --hive-delims-replacement " " \
            --delete-target-dir \
            --compression-codec=snappy
        '''.format(
            host=conn_conf_dict[conn_id].host,
            port=conn_conf_dict[conn_id].port,
            schema=db_name,
            username=conn_conf_dict[conn_id].login,
            password=conn_conf_dict[conn_id].password,
            table=table_name,
            ufile_path=UFILE_PATH % (db_name, table_name)
        ),
        dag=dag,
    )

    # check table
    check_table = PythonOperator(
        task_id='check_table_{}'.format(hive_table_name),
        python_callable=run_check_table,
        provide_context=True,
        op_kwargs={
            'db_name': db_name,
            'table_name': table_name,
            'conn_id': conn_id,
            'hive_table_name': hive_table_name
        },
        dag=dag
    )
    # add partitions
    add_partitions = HiveOperator(
        task_id='add_partitions_{}'.format(hive_table_name),
        hql='''
                ALTER TABLE {table} ADD IF NOT EXISTS PARTITION (dt = '{{{{ ds }}}}');
            '''.format(table=hive_table_name),
        schema=HIVE_DB,
        dag=dag)

    # 数据量监控
    volume_monitoring = PythonOperator(
        task_id='volume_monitorin_{}'.format(hive_table_name),
        python_callable=data_volume_monitoring,
        provide_context=True,
        op_kwargs={
            'db_name': HIVE_DB,
            'table_name': hive_table_name,
            'is_valid_success':"true"
        },
        dag=dag
    )
    import_table >> volume_monitoring

    # 超时监控
    task_timeout_monitor= PythonOperator(
        task_id='task_timeout_monitor_{}'.format(hive_table_name),
        python_callable=fun_task_timeout_monitor,
        provide_context=True,
        op_kwargs={
            'db_name': HIVE_DB,
            'table_name': hive_table_name,
        },
        dag=dag_monitor
    )


    check_table >> add_partitions >> import_table
